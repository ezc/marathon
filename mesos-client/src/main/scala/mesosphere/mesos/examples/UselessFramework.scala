package mesosphere.mesos.examples

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import mesosphere.mesos.client.MesosClient
import mesosphere.mesos.conf.MesosConf
import org.apache.mesos.v1.mesos.{Filters, FrameworkID, FrameworkInfo, OfferID}
import org.apache.mesos.v1.scheduler.scheduler.Event

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}

object UselessFramework extends App with StrictLogging {

  /** Run Foo framework that:
    *  - successfully subscribes
    *  - declines all offers.
    *
    *  Not much, but shows the basic idea. Good to test against local mesos.
    *
    */
  override def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val frameworkInfo = FrameworkInfo(
      user = "foo",
      name = "Example FOO Framework",
      roles = Seq("test"),
      capabilities = Seq(FrameworkInfo.Capability(`type` = Some(FrameworkInfo.Capability.Type.MULTI_ROLE)))
    )

    val conf = new MesosConf(List("--master", s"127.0.0.1:5050"))
    val client = new MesosClient(conf, frameworkInfo)

    val frameworkIDP = Promise[FrameworkID]()
    val frameworkIDF = frameworkIDP.future

    client.mesosSource.runWith(Sink.foreach { event =>

      if (event.`type`.get == Event.Type.SUBSCRIBED) {  // Save frameworkdId received
        frameworkIDP.success(event.subscribed.get.frameworkId)
      }
      else if (event.`type`.get == Event.Type.OFFERS) { // Test case: decline first offer received

        val frameworkID = Await.result(frameworkIDF, Duration.Inf)  // Should be instant and in the same thread since frameworkIDF is already completed
        val offerIds = event.offers.get.offers.map(_.id).toList

        Source(offerIds)
          .map{ oId => logger.info(s"Declining offer with id = ${oId.value}"); oId }
          .map(oId => client.decline(
              frameworkId = frameworkID,
              offerIds = Seq(oId),
              filters = Some(Filters(Some(5.0f)))
            ))
          .runWith(client.mesosSink)
      }

    }).onComplete{
      case Success(res) =>
        logger.info(s"Stream completed: $res"); system.terminate()
      case Failure(e) => logger.error(s"Error in stream: $e"); system.terminate()
    }
  }
}
