package mesosphere.mesos.client

import akka.http.scaladsl.model.{ContentType, MediaType}
import akka.http.scaladsl.model.MediaType.Compressible

object MesosStreamSupport {

  val MesosStreamIdHeader = "Mesos-Stream-Id"
  val ProtobufMediaType: MediaType.Binary = MediaType.applicationBinary("x-protobuf", Compressible)
  val ProtobufContentType: ContentType = ContentType.Binary(ProtobufMediaType)
}
