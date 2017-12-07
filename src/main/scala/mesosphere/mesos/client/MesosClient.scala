package mesosphere.mesos.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.stream.ActorMaterializer
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
import akka.stream.scaladsl._
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon
import mesosphere.marathon.{AllConf, MarathonConf}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class MesosClient(conf: MarathonConf)(
    implicit val system: ActorSystem,
    implicit val materializer: ActorMaterializer,
    implicit val executionContext: ExecutionContext
) extends StrictLogging {

  /**
    * Inbound traffic is handled via a persistent connection to the `/api/v1/scheduler` endpoint.
    * Each message is encoded in RecordIO format, which essentially prepends to a single record
    * (either JSON or serialized protobuf) its length in bytes: [<length>\n<json string|protobuf bytes>]
    *
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#recordio-response-format-1
    */
  val Array(host, port) = conf.mesosMaster().split(":")
  logger.info(s"Connecting to mesos master: $host:$port")

  val httpConnection: Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] = Http().outgoingConnection(host, port.toInt)
  val recordIoScanner = RecordIOFraming.scanner()

  val overflowStrategy = akka.stream.OverflowStrategy.backpressure

  val streamIdPromise = Promise[String]()
  val mesosStreamId: Future[String] = streamIdPromise.future
  mesosStreamId.foreach(id => logger.info(s"MesosStreamId: $id"))

  def dispatchRequest(request: HttpRequest): Future[HttpResponse] = ???

  /**
    * Subscribe call should be the first or the client to make:
    *
    * SUBSCRIBE Request (JSON):
    *
    * POST /api/v1/scheduler  HTTP/1.1
    *
    * Host: masterhost:5050
    * Content-Type: application/json
    * Accept: application/json
    * Connection: close
    *
    * {
    *    "type"       : "SUBSCRIBE",
    *    "subscribe"  : {
    *       "framework_info"  : {
    *         "user" :  "foo",
    *         "name" :  "Example HTTP Framework",
    *         "roles": ["test"],
    *         "capabilities" : [{"type": "MULTI_ROLE"}]
    *       }
    *   }
    * }
    *
    * SUBSCRIBE Response Event (JSON):
    * HTTP/1.1 200 OK
    *
    * Content-Type: application/json
    * Transfer-Encoding: chunked
    * Mesos-Stream-Id: 130ae4e3-6b13-4ef4-baa9-9f2e85c3e9af
    *
    * <event length>
    * {
    *  "type"         : "SUBSCRIBED",
    *  "subscribed"   : {
    *      "framework_id"               : {"value":"12220-3440-12532-2345"},
    *      "heartbeat_interval_seconds" : 15
    *   }
    * }
    * <more events>
    * http://mesos.apache.org/documentation/latest/scheduler-http-api/#subscribe-1
    */
  private def subscribe(): Source[String, NotUsed] = {
    val MesosStreamId = "Mesos-Stream-Id"

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

    logger.info(s"Subscribing: $request")

    def subscribedHandler(streamId: Promise[String]): Flow[HttpResponse, HttpResponse, NotUsed] = {
      Flow[HttpResponse]
        .map { res =>
          res.status match {
            case StatusCodes.OK =>
              logger.info(s"Connected successfully to ${conf.mesosMaster()}");
              val mesosStreamId = res.headers
                .find(_.name() == MesosStreamId)
                .map(_.value())
                .getOrElse(throw new IllegalStateException("Subscribe always returns a streamId"))
              streamId.success(mesosStreamId)
              res
            case StatusCodes.TemporaryRedirect =>
              throw new IllegalArgumentException(s"${conf.mesosMaster} is unavailable") // Handle a redirect to a current leader
            case _ =>
              throw new IllegalArgumentException(s"Mesos server error: $res")
          }
        }
    }

    Source.single(request)
      .via(httpConnection)
      .via(subscribedHandler(streamIdPromise))
      .flatMapConcat(_.entity.dataBytes)
      .map{ e => logger.info(s"HttpEntity $e"); e }
      .via(recordIoScanner)
      .map(_.utf8String)
      .map{ e => logger.info(s"Mesos Event: $e"); e }
      .toMat(BroadcastHub.sink)(Keep.right).run()
  }
}

trait MesosApi { this: MesosClient =>
}

object MesosClient extends StrictLogging {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val conf: MarathonConf = new AllConf(args.to[marathon.Seq])
    val client = new MesosClient(conf)

    client.subscribe().runWith(Sink.head).onComplete{
      case Success(res) => logger.info(s"res")
      case Failure(e) => logger.error(s"$e")
    }

    // Let's wait
    scala.io.StdIn.readLine()
  }
}
