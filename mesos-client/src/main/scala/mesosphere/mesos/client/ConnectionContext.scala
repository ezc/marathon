package mesosphere.mesos.client

import mesosphere.mesos.conf.MesosConf

case class ConnectionContext(host: String, port: Int, mesosStreamId: String) {
  def url = s"$host:$port"
}


object ConnectionContext {
  def apply(conf: MesosConf): ConnectionContext = ConnectionContext(conf.mesosMasterHost, conf.mesosMasterPort, "")
}