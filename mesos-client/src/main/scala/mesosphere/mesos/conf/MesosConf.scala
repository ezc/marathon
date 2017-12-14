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

  val sourceBufferSize = opt[Int](
    "buffer_size",
    descr = "Buffer size of the mesos source",
    required = false,
    noshort = true,
    hidden = true,
    default = Some(10)
  )

  verify()

  val mesosMasterHost:String = mesosMaster().split(":")(0)

  val mesosMasterPort:Int = mesosMaster().split(":")(1).toInt
}