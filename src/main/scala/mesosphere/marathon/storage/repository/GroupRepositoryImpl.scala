package mesosphere.marathon.storage.repository

// scalastyle:off
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.{ Done, NotUsed }
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.Protos
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.core.storage.repository.impl.PersistenceStoreVersionedRepository
import mesosphere.marathon.core.storage.store.impl.BasePersistenceStore
import mesosphere.marathon.core.storage.store.impl.cache.{ LazyCachingPersistenceStore, LoadTimeCachingPersistenceStore }
import mesosphere.marathon.core.storage.store.{ IdResolver, PersistenceStore }
import mesosphere.marathon.state.{ AppDefinition, Group, PathId, Timestamp }
import mesosphere.marathon.util.{ RichLock, toRichFuture }

import scala.async.Async.{ async, await }
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }
// scalastyle:on

private[storage] case class StoredGroup(
    id: PathId,
    appIds: Map[PathId, OffsetDateTime],
    podIds: Map[PathId, OffsetDateTime],
    storedGroups: Seq[StoredGroup],
    dependencies: Set[PathId],
    version: OffsetDateTime) {

  lazy val transitiveAppIds: Map[PathId, OffsetDateTime] = appIds ++ storedGroups.flatMap(_.appIds)
  lazy val transitivePodIds: Map[PathId, OffsetDateTime] = podIds ++ storedGroups.flatMap(_.podIds)

  def resolve(
    appRepository: AppRepository,
    podRepository: PodRepository)(implicit ctx: ExecutionContext): Future[Group] = async {
    val appFutures = appIds.map { case (appId, appVersion) => appRepository.getVersion(appId, appVersion) }
    val podFutures = podIds.map { case (podId, podVersion) => podRepository.getVersion(podId, podVersion) }

    val groupFutures = storedGroups.map(_.resolve(appRepository, podRepository))

    val apps: Map[PathId, AppDefinition] = await(Future.sequence(appFutures)).collect {
      case Some(app: AppDefinition) =>
        app.id -> app
    }(collection.breakOut)

    val pods: Map[PathId, PodDefinition] = await(Future.sequence(podFutures)).collect {
      case Some(pod: PodDefinition) =>
        pod.id -> pod
    }(collection.breakOut)

    val groups = await(Future.sequence(groupFutures)).toSet

    Group(
      id = id,
      apps = apps,
      pods = pods,
      groups = groups,
      dependencies = dependencies,
      version = Timestamp(version)
    )
  }

  def toProto: Protos.GroupDefinition = {
    import StoredGroup.DateFormat

    val apps = appIds.map {
      case (app, appVersion) =>
        Protos.GroupDefinition.AppReference.newBuilder()
          .setId(app.safePath)
          .setVersion(DateFormat.format(appVersion))
          .build()
    }

    Protos.GroupDefinition.newBuilder
      .setId(id.safePath)
      .addAllApps(apps.asJava)
      .addAllGroups(storedGroups.map(_.toProto).asJava)
      .addAllDependencies(dependencies.map(_.safePath).asJava)
      .setVersion(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(version))
      .build()
  }
}

object StoredGroup {
  val DateFormat = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  def apply(group: Group): StoredGroup =
    StoredGroup(
      id = group.id,
      appIds = group.apps.mapValues(_.version.toOffsetDateTime),
      podIds = group.pods.mapValues(_.version.toOffsetDateTime),
      storedGroups = group.groups.map(StoredGroup(_))(collection.breakOut),
      dependencies = group.dependencies,
      version = group.version.toOffsetDateTime)

  def apply(proto: Protos.GroupDefinition): StoredGroup = {
    val apps: Map[PathId, OffsetDateTime] = proto.getAppsList.asScala.map { appId =>
      PathId.fromSafePath(appId.getId) -> OffsetDateTime.parse(appId.getVersion, DateFormat)
    }(collection.breakOut)

    val pods: Map[PathId, OffsetDateTime] = proto.getPodsList.asScala.map { podId =>
      PathId.fromSafePath(podId.getId) -> OffsetDateTime.parse(podId.getVersion, DateFormat)
    }(collection.breakOut)

    val groups = proto.getGroupsList.asScala.map(StoredGroup(_))

    StoredGroup(
      id = PathId.fromSafePath(proto.getId),
      appIds = apps,
      podIds = pods,
      storedGroups = groups.toVector,
      dependencies = proto.getDependenciesList.asScala.map(PathId.fromSafePath)(collection.breakOut),
      version = OffsetDateTime.parse(proto.getVersion, DateFormat)
    )
  }
}

class StoredGroupRepositoryImpl[K, C, S](
    persistenceStore: PersistenceStore[K, C, S],
    appRepository: AppRepository,
    podRepository: PodRepository)(
    implicit
    ir: IdResolver[PathId, StoredGroup, C, K],
    marshaller: Marshaller[StoredGroup, S],
    unmarshaller: Unmarshaller[S, StoredGroup],
    val ctx: ExecutionContext,
    val mat: Materializer
) extends GroupRepository with StrictLogging {
  import StoredGroupRepositoryImpl._

  /*
  Basic strategy for caching:
  get -> "wait" on the future, if it fails, create a new promise for it and actually fetch the root,
  completing the promise with the fetch result.
  set -> create a new promise for the root. If store succeeds, go update it, if it doesn't
         complete the new future with the result of the previous root future.

  This gives us read-after-write consistency.
   */
  private val lock = RichLock()
  private var rootFuture = Future.failed[Group](new Exception)
  private[storage] var beforeStore = Option.empty[(StoredGroup) => Future[Done]]

  private val storedRepo = {
    def leafStore(store: PersistenceStore[K, C, S]): PersistenceStore[K, C, S] = store match {
      case s: BasePersistenceStore[K, C, S] => s
      case s: LoadTimeCachingPersistenceStore[K, C, S] => leafStore(s.store)
      case s: LazyCachingPersistenceStore[K, C, S] => leafStore(s.store)
    }
    new PersistenceStoreVersionedRepository[PathId, StoredGroup, K, C, S](leafStore(persistenceStore), _.id, _.version)
  }

  private[storage] def underlyingRoot(): Future[Group] = async {
    val root = await(storedRepo.get(RootId))
    val resolved = root.map(_.resolve(appRepository, podRepository))
    resolved match {
      case Some(x) => await(x)
      case None => Group.empty
    }
  }

  override def root(): Future[Group] =
    async {
      await(lock(rootFuture).asTry) match {
        case Failure(_) =>
          val promise = Promise[Group]()
          lock {
            rootFuture = promise.future
          }
          val unresolved = await(storedRepo.get(RootId))
          val newRoot = unresolved.map(_.resolve(appRepository, podRepository)) match {
            case Some(group) =>
              await(group)
            case None =>
              Group.empty
          }
          promise.success(newRoot)
          newRoot
        case Success(root) =>
          root
      }
    }

  override def rootVersions(): Source[OffsetDateTime, NotUsed] =
    storedRepo.versions(RootId)

  override def rootVersion(version: OffsetDateTime): Future[Option[Group]] = async {
    val unresolved = await(storedRepo.getVersion(RootId, version))
    unresolved.map(_.resolve(appRepository, podRepository)) match {
      case Some(group) =>
        Some(await(group))
      case None =>
        None
    }
  }

  override def storeRoot(group: Group, updatedApps: Seq[AppDefinition], deletedApps: Seq[PathId],
    updatedPods: Seq[PodDefinition], deletedPods: Seq[PathId]): Future[Done] =
    async {
      val storedGroup = StoredGroup(group)
      beforeStore match {
        case Some(preStore) =>
          await(preStore(storedGroup))
        case _ =>
      }
      val promise = Promise[Group]()
      val oldRootFuture = lock {
        val old = rootFuture
        rootFuture = promise.future
        old
      }
      val storeAppFutures = updatedApps.map(appRepository.store)
      val storePodFutures = updatedPods.map(podRepository.store)
      val deleteAppFutures = deletedApps.map(appRepository.deleteCurrent)
      val deletePodFutures = deletedPods.map(podRepository.deleteCurrent)
      val storedApps = await(Future.sequence(storeAppFutures).asTry)
      val storedPods = await(Future.sequence(storePodFutures).asTry)
      await(Future.sequence(deleteAppFutures).recover { case NonFatal(e) => Done })
      await(Future.sequence(deletePodFutures).recover { case NonFatal(e) => Done })

      def revertRoot(ex: Throwable): Done = {
        promise.completeWith(oldRootFuture)
        throw ex
      }

      storedApps match {
        case Success(_) =>
          val storedRoot = await(storedRepo.store(storedGroup).asTry)
          storedRoot match {
            case Success(_) =>
              promise.success(group)
              Done
            case Failure(ex) =>
              logger.error(s"Unable to store updated group $group", ex)
              revertRoot(ex)
          }
        case Failure(ex) =>
          logger.error(s"Unable to store updated apps or pods: " +
            s"${updatedApps.map(_.id).mkString} ${updatedPods.map(_.id).mkString}", ex)
          revertRoot(ex)
      }
    }

  private[storage] def lazyRootVersion(version: OffsetDateTime): Future[Option[StoredGroup]] = {
    storedRepo.getVersion(RootId, version)
  }

  private[storage] def deleteRootVersion(version: OffsetDateTime): Future[Done] = {
    persistenceStore.deleteVersion(RootId, version)
  }
}

object StoredGroupRepositoryImpl {
  val RootId = PathId.empty
}