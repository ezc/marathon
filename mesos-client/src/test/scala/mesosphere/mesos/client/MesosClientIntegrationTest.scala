package mesosphere.mesos.client

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.scalalogging.StrictLogging
import mesosphere.AkkaUnitTest
import mesosphere.marathon.IntegrationTest
import mesosphere.marathon.integration.setup.MesosClusterTest
import mesosphere.mesos.conf.MesosConf
import org.apache.mesos.v1.mesos.{FrameworkID, FrameworkInfo}
import org.apache.mesos.v1.scheduler.scheduler.Event
import org.scalatest.Inside
import org.scalatest.concurrent.Eventually

import scala.concurrent.Future
import scala.util.Success

@IntegrationTest
class MesosClientIntegrationTest extends AkkaUnitTest
  with MesosClusterTest
  with Eventually
  with Inside
  with StrictLogging {

  "Mesos client should successfully subscribe to mesos" in {
    val f = new Fixture()

    val res: Future[Seq[Event]] = f.client.mesosSource.take(1).runWith(Sink.seq)
    eventually {
      inside(res.value) {
        case Some(Success(events)) =>
          events.head.`type` shouldBe Some(Event.Type.SUBSCRIBED)
      }
    }
  }

  "Mesos client should successfully receive heartbeats" in {
    val f = new Fixture()

    val res: Future[Seq[Event]] = f.client.mesosSource.take(2).runWith(Sink.seq)
    eventually {
      inside(res.value) {
        case Some(Success(events)) =>
          events.filter(_.`type` == Some(Event.Type.HEARTBEAT)).size should be > 0
      }
    }
  }

  "Mesos client should successfully receive offers" in {
    val f = new Fixture()

    val res: Future[Seq[Event]] = f.client.mesosSource.take(3).runWith(Sink.seq)
    eventually {
      inside(res.value) {
        case Some(Success(events)) =>
          events.filter(_.`type` == Some(Event.Type.OFFERS)).size should be > 0
      }
    }
  }

  class Fixture() {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val frameworkID = FrameworkID("84b4efc0-545c-485c-a044-7c8e52b67379-1111")
    val frameworkInfo = FrameworkInfo(
      user = "foo",
      name = "Example FOO Framework",
      id = Some(frameworkID),
      roles = Seq("foo"),
      capabilities = Seq(FrameworkInfo.Capability(`type` = Some(FrameworkInfo.Capability.Type.MULTI_ROLE)))
    )

    val mesosUrl = new java.net.URI(mesos.url)

    val conf = new MesosConf(List("--master", s"${mesosUrl.getHost}:${mesosUrl.getPort}"))
    val client = new MesosClient(conf, frameworkInfo)
  }
}
