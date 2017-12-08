package mesosphere.mesos.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.stream.ActorMaterializer
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon
import mesosphere.marathon.{AllConf, MarathonConf}
import org.apache.mesos.v1.Protos.FrameworkInfo

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

  /**
    * Subscribe call should be the first or the client to make. It will initialize the connection to mesos
    * and return a `Source[String, NotUser]` with mesos events. It is declared lazy to decouple instantiation of the
    * MesosClient instance from connection initialization. All subsequent calls will return the previously created
    * event source.
    * The connection is initialized with a POST /api/v1/scheduler with the framework info in the body. The request
    * is answered by a SUBSCRIBED event which contains MesosStreamId header. This is reused by all later calls to
    * /api/v1/scheduler.
    * Multiple subscribers can attach to returned `Source[String, NotUsed]` to receive mesos Events. The stream will
    * be closed either on connection error or connection shutdown e.g.:
    * ```
    * client.subscribe.runWith(Sink.ignore).onComplete{
    *  case Success(res) => logger.info(s"Stream completed: $res")
    *  case Failure(e) => logger.error(s"Error in stream: $e")
    * }
    * ```
    *
    * Mesos documentation on SUBSCRIBE call:
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#subscribe-1
    */
  lazy val subscribe: Source[String, NotUsed] = {
    // TODO: build body from FrameworkInfo
    val body =
      """
        |{
        |  "type"       : "SUBSCRIBE",
        |  "subscribe"  : {
        |    "framework_info"  : {
        |      "user" :  "foo",
        |      "name" :  "Example HTTP Framework",
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

    val messageFlow = Flow[HttpRequest]
      .via(log(s"Connecting to mesos master: $host:$port"))
      .via(httpConnection)
      .via(log("HttpResponse: "))
      .via(subscribedHandler)
      .via(entityBytesExtractor)
      .via(recordIoScanner)
      .via(eventDeserializer)
      .via(log("Mesos Event: "))

    Source.single(request)
      .via(messageFlow)
      .toMat(BroadcastHub.sink)(Keep.right).run()
  }
}

trait MesosApi { this: MesosClient =>

}

// TODO: PLAN
// TODO: Extract into a marathon sub-project
// TODO: Add v1 protobuf files and use scalapb (https://scalapb.github.io/) to create scala case classes
// TODO: Set FrameworkInfo on SUBSCRIBE request
// TODO: Switch from `application/json` to `application/x-protobuf`
// TODO: Provide a Sink[Call, NotUsed] to make calls to mesos `api/v1/scheduler`
// TODO: Extract API trait

object MesosClient extends StrictLogging {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val conf: MarathonConf = new AllConf(args.to[marathon.Seq])
    val client = new MesosClient(conf, null)

    client.subscribe.runWith(Sink.ignore).onComplete{
      case Success(res) => logger.info(s"Stream completed: $res"); system.terminate()
      case Failure(e) => logger.error(s"Error in stream: $e"); system.terminate()
    }
  }
}
