package mesosphere.mesos.client

import org.rogach.scallop.ScallopConf

/**
  * Configuring mesos v1 client
  */
trait MesosConf extends ScallopConf {

  lazy val mesosClientRequestQueueSize = opt[Int](
    "mesos_client_request_queue_size",
    descr = "INTERNAL TUNING PARAMETER: Size of the mesos v1 client request queue",
    hidden = true,
    default = Some(1024))
}
