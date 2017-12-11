package mesosphere.mesos.conf

import org.rogach.scallop.ScallopConf

/**
  * Configuring mesos v1 client
  */
class MesosConf(args: Seq[String]) extends ScallopConf(args) {

  val mesosMaster = opt[String](
    "master",
    descr = "The URL of the Mesos master",
    required = true,
    noshort = true)

  verify()
}