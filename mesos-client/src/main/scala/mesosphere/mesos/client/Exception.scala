package mesosphere.mesos.client
import java.net.URI

case class MesosRedicrectException(leader: URI) extends Exception(s"New mesos leader available at $leader")
