package mesosphere.mesos.client

import java.util.UUID

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

@IntegrationTest
class MesosClientIntegrationTest extends AkkaUnitTest
  with MesosClusterTest
  with Eventually
  with Inside
  with StrictLogging {

  "Mesos client should successfully subscribe to mesos without framework Id" in withFixture() { f =>

    val res: Future[Seq[Event]] = f.client.mesosSource.take(1).runWith(Sink.seq)
    inside(res.futureValue) {
      case events =>
        events.head.`type` shouldBe Some(Event.Type.SUBSCRIBED)
    }
  }

  "Mesos client should successfully subscribe to mesos with framework Id" in {
    val frameworkID = FrameworkID(UUID.randomUUID().toString)

    withFixture(Some(frameworkID)) { f =>
      val res: Future[Seq[Event]] = f.client.mesosSource.take(1).runWith(Sink.seq)
      inside(res.futureValue) {
        case events =>
          events.head.`type` shouldBe Some(Event.Type.SUBSCRIBED)
          events.head.subscribed.get.frameworkId shouldBe frameworkID
      }
    }
  }

  "Mesos client should successfully receive heartbeats" in withFixture() { f =>
    val res: Future[Seq[Event]] = f.client.mesosSource.take(2).runWith(Sink.seq)
    inside(res.futureValue) {
      case events =>
        events.filter(_.`type` == Some(Event.Type.HEARTBEAT)).size should be > 0
    }
  }

  "Mesos client should successfully receive offers" in withFixture() { f =>
    val res: Future[Seq[Event]] = f.client.mesosSource.take(3).runWith(Sink.seq)
    inside(res.futureValue) {
      case events =>
        events.filter(_.`type` == Some(Event.Type.OFFERS)).size should be > 0
    }
  }

  def withFixture(frameworkId: Option[FrameworkID] = None)(fn: Fixture => Unit): Unit = {
    val f = new Fixture(frameworkId)
    try (fn(f)) finally {
      f.client.killSwitch.shutdown()
    }
  }

  class Fixture(frameworkId: Option[FrameworkID] = None) {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val frameworkInfo = FrameworkInfo(
      user = "foo",
      name = "Example FOO Framework",
      id = frameworkId,
      roles = Seq("foo"),
      failoverTimeout = Some(0.0f),
      capabilities = Seq(FrameworkInfo.Capability(`type` = Some(FrameworkInfo.Capability.Type.MULTI_ROLE)))
    )

    val mesosUrl = new java.net.URI(mesos.url)

    val conf = new MesosConf(List("--master", s"${mesosUrl.getHost}:${mesosUrl.getPort}"))
    val client = new MesosClient(conf, frameworkInfo)
  }
}
