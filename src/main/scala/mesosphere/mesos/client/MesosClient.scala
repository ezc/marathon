package mesosphere.mesos.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, RawHeader}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon
import mesosphere.marathon.{AllConf, MarathonConf}
import org.apache.mesos.v1.Protos.FrameworkInfo
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class MesosClient(
  conf: MarathonConf,
    frameworkInfo: FrameworkInfo)(
    implicit
    val system: ActorSystem,
    implicit val materializer: ActorMaterializer,
    implicit val executionContext: ExecutionContext
) extends MesosApi with StrictLogging {

  val Array(host, port) = conf.mesosMaster().split(":")

  val overflowStrategy = akka.stream.OverflowStrategy.backpressure

  val mesosStreamIdHeader = "Mesos-Stream-Id"
  val mesosStreamIdPromise = Promise[String]()
  val mesosStreamId: Future[String] = mesosStreamIdPromise.future

  /** It is declared lazy to decouple instantiation of the MesosClient instance from connection initialization. */
  override lazy val source: Source[String, NotUsed] = {
    // TODO: build body from FrameworkInfo
    val body =
      """
        |{
        |  "type"       : "SUBSCRIBE",
        |  "framework_id"   : {"value" : "84b4efc0-545c-485c-a044-7c8e52b67379-1111"},
        |  "subscribe"  : {
        |    "framework_info"  : {
        |      "user" :  "foo",
        |      "name" :  "Example HTTP Framework",
        |      "id"   : {"value" : "84b4efc0-545c-485c-a044-7c8e52b67379-1111"},
        |      "roles": ["test"],
        |      "capabilities" : [{"type": "MULTI_ROLE"}]
        |    }
        |  }
        |}
      """.stripMargin

    val request = HttpRequest(
      HttpMethods.POST,
      uri = Uri("/api/v1/scheduler"),
      entity = HttpEntity(MediaTypes.`application/json`, body),
      headers = List(Accept(MediaTypes.`application/json`)))

    val httpConnection: Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] = Http().outgoingConnection(host, port.toInt)

    /**
      * Inbound traffic is handled via a persistent connection to the `/api/v1/scheduler` endpoint.
      * Each message is encoded in RecordIO format, which essentially prepends to a single record
      * (either JSON or serialized protobuf) its length in bytes: [<length>\n<json string|protobuf bytes>]
      *
      * http://mesos.apache.org/documentation/latest/scheduler-http-api/#recordio-response-format-1
      */
    val recordIoScanner: Flow[ByteString, ByteString, NotUsed] = RecordIOFraming.scanner()

    val subscribedHandler: Flow[HttpResponse, HttpResponse, NotUsed] = Flow[HttpResponse].map { res =>
      res.status match {
        case StatusCodes.OK =>
          logger.info(s"Connected successfully to ${conf.mesosMaster()}");
          val mesosStreamId = res.headers
            .find(_.name() == mesosStreamIdHeader)
            .map(_.value())
            .getOrElse(throw new IllegalStateException(s"Missing MesosStreamId header in ${res.headers}"))
          mesosStreamIdPromise.success(mesosStreamId)
          res
        case StatusCodes.TemporaryRedirect =>
          throw new IllegalArgumentException(s"${conf.mesosMaster} is unavailable") // Handle a redirect to a current leader
        case _ =>
          throw new IllegalArgumentException(s"Mesos server error: $res")
      }
    }

    val entityBytesExtractor: Flow[HttpResponse, ByteString, NotUsed] = Flow[HttpResponse].flatMapConcat(_.entity.dataBytes)

    val eventDeserializer: Flow[ByteString, String, NotUsed] = Flow[ByteString].map(_.utf8String)

    def log[T](prefix: String): Flow[T, T, NotUsed] = Flow[T].map{e => logger.info(s"$prefix$e"); e}

    val flow = Flow[HttpRequest]
      .via(log(s"Connecting to mesos master: $host:$port"))
      .via(httpConnection)
      .via(log("HttpResponse: "))
      .via(subscribedHandler)
      .via(entityBytesExtractor)
      .via(recordIoScanner)
      .via(eventDeserializer)
      .via(log("Mesos Event: "))

    Source.single(request)
      .via(flow)
      .toMat(BroadcastHub.sink)(Keep.right).run()
  }

  // ***************** Outbound mesos connections ******************
  // A sink for mesos events. We use `Http().singleRequest()` method thus creating a new HTTP request every time.
  private val mesosSink: Sink[(String, String), Future[Done]] = Sink.foreach[(String, String)]{case (mesosStreamId, call) =>

    logger.info(s"Sending: $call")

    val request = HttpRequest(
      HttpMethods.POST,
      uri = Uri(s"http://$host:$port/api/v1/scheduler"),
      entity = HttpEntity(MediaTypes.`application/json`, call),
      headers = List(Accept(MediaTypes.`application/json`), RawHeader("Mesos-Stream-Id", mesosStreamId)))

    Http().singleRequest(request).map{response =>
      logger.info(s"Response: $response")
      response.discardEntityBytes()}
  }

  // Attach a MergeHub Source to mesos sink. This will materialize to a corresponding Sink.
  private val sinkHub: RunnableGraph[Sink[String, NotUsed]] =
    MergeHub.source[String](perProducerBufferSize = 16)
      .mapAsync(1)(e => mesosStreamId.map(id => (id, e)))
      .to(mesosSink)

  // By running/materializing the consumer we get back a Sink, and hence now have access to feed elements into it.
  // This Sink can be materialized any number of times, and every element that enters the Sink will be consumed by
  // our consumer.
  override val sink: Sink[String, NotUsed] = sinkHub.run()
}

trait MesosApi {

  /**
    * First call to this method will initialize the connection to mesos and return a `Source[String, NotUser]` with
    * mesos `Event`s. All subsequent calls will return the previously created event source.
    * The connection is initialized with a `POST /api/v1/scheduler` with the framework info in the body. The request
    * is answered by a `SUBSCRIBED` event which contains `MesosStreamId` header. This is reused by all later calls to
    * `/api/v1/scheduler`.
    * Multiple subscribers can attach to returned `Source[String, NotUsed]` to receive mesos events. The stream will
    * be closed either on connection error or connection shutdown e.g.:
    * ```
    * client.source.runWith(Sink.ignore).onComplete{
    *  case Success(res) => logger.info(s"Stream completed: $res")
    *  case Failure(e) => logger.error(s"Error in stream: $e")
    * }
    * ```
    *
    * Mesos documentation on SUBSCRIBE call:
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#subscribe-1
    */
  def source: Source[String, NotUsed]

  /** Sink for mesos calls. Multiple publishers can materialize this sink to send mesos `Call`s. Every `Call` is sent
    * using a new HTTP connection.
    * Note: a scheduler can't send calls to mesos without subscribing first (see [MesosClient.source] method). Calls
    * published to sink without a successful subscription will be buffered and will have to wait for subscription
    * connection. Always call `source()` first.
    *
    * More on mesos `Call`s:
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#calls
    */
  def sink: Sink[String, NotUsed]
}

// TODO: PLAN
// TODO: Extract into a marathon sub-project
// TODO: Add v1 protobuf files and use scalapb (https://scalapb.github.io/) to create scala case classes
// TODO: Set FrameworkInfo on SUBSCRIBE request
// TODO: Switch from `application/json` to `application/x-protobuf`
// TODO: Extract API trait

object MesosClient extends StrictLogging {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val conf: MarathonConf = new AllConf(args.to[marathon.Seq])
    val client = new MesosClient(conf, null)

    client.source.runWith(Sink.foreach { event =>

      val offerJson: JsValue = Json.parse(event)
      val eventType = (offerJson \ "type").as[String]

      // Test case: decline first offer received
      if (eventType == "OFFERS") {
        val offerId = ((offerJson \ "offers" \ "offers").as[List[JsObject]].head \ "id" \ "value").as[String]
        logger.info(s"Declining offer with $offerId")
        val decline = s"""
                         |{
                         |  "framework_id"    : {"value" : "84b4efc0-545c-485c-a044-7c8e52b67379-1111"},
                         |  "type"            : "DECLINE",
                         |  "decline"         : {
                         |    "offer_ids" : [
                         |                   {"value" : "$offerId"}
                         |                  ],
                         |    "filters"   : {"refuse_seconds" : 5.0}
                         |  }
                         |}""".stripMargin
        Source.single(decline).runWith(client.sink)
      }

    }).onComplete{
      case Success(res) => logger.info(s"Stream completed: $res"); system.terminate()
      case Failure(e) => logger.error(s"Error in stream: $e"); system.terminate()
    }
  }
}
