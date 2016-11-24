package org.apache.mesos.chronos.scheduler.jobs.stats

import scala.collection._
import java.util.logging.{Level, Logger}

import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection}
import org.mongodb.scala.bson.collection.mutable.{Document => MutableDocument}
import org.mongodb.scala.model.Filters
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Updates.inc
import org.mongodb.scala.model.Indexes._
import com.google.inject.Inject
import org.apache.mesos.Protos.{TaskState, TaskStatus}
import org.apache.mesos.chronos.scheduler.config.MongoConfiguration
import org.apache.mesos.chronos.scheduler.jobs._
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object CurrentState extends Enumeration {
  type CurrentState = Value
  val idle, queued, running = Value
}

class JobStats @Inject()(config: MongoConfiguration) {

  // Mongodb document column names
  private val ATTEMPT: String = "attempt"
  private val ELEMENTS_PROCESSED: String = "elements_processed"
  private val IS_FAILURE: String = "is_failure"
  private val JOB_NAME: String = "job_name"
  private val JOB_OWNER: String = "job_owner"
  private val JOB_PARENTS: String = "job_parents"
  private val JOB_SCHEDULE: String = "job_schedule"
  private val MESSAGE: String = "message"
  private val SLAVE_ID: String = "slave_id"
  private val TASK_ID: String = "id"
  private val TASK_STATE: String = "task_state"
  private val TIMESTAMP: String = "ts"

  protected val jobStates = new mutable.HashMap[String, CurrentState.Value]()

  val log = Logger.getLogger(getClass.getName)
  var _mongoClient: Option[MongoClient] = None

  def getJobState(jobName: String): CurrentState.Value = {
    /**
      * NOTE: currently everything stored in memory, look into moving
      * this to Cassandra. ZK is not an option cause serializers and
      * deserializers need to be written. Need a good solution, potentially
      * lots of writes and very few reads (only on failover)
      */
    jobStates.getOrElse(jobName, CurrentState.idle)
  }

  def updateJobState(jobName: String, nextState: CurrentState.Value) {
    val shouldUpdate = jobStates.get(jobName).map {
      currentState =>
        !(currentState == CurrentState.running && nextState == CurrentState.queued)
    }.getOrElse(true)

    if (shouldUpdate) {
      log.info("Updating state for job (%s) to %s".format(jobName, nextState))
      jobStates.put(jobName, nextState)
    }
  }

  private def removeJobState(job: BaseJob) = jobStates.remove(job.name)

  /**
    * Queries Mongo collection for past and current job statistics by jobName
    * and limits by numTasks. The result is not sorted by execution time
    *
    * @param job job to find task data for
    * @return list of Mongo documents
    */
  private def getTaskDataByJob(job: BaseJob): Option[List[Document]] = {
    var rowsListFinal: Option[List[Document]] = None

    getClient match {
      case Some(client: MongoClient) =>
        val collection = client.getDatabase(config.mongoDatabase()).getCollection(config.mongoCollection())
        val query = collection.find(Filters.equal(JOB_NAME, job.name)).toFuture()

        try {
          // FIXME: better await timeout value
          val docs = Await.result(query, Duration.Inf)
          rowsListFinal = Some(docs.toList)
        } catch {
          case ex: java.util.concurrent.TimeoutException => log.log(Level.WARNING, "query Mongodb timeout", ex)
          case ex: Throwable =>
            log.log(Level.WARNING, "query Mongodb error", ex)
            closeClient()
        }
      case None => {}
    }
    rowsListFinal
  }

  /**
    * Compare function for TaskStat by most recent date.
    */
  private def recentDateCompareFnc(a: TaskStat, b: TaskStat): Boolean = {
    val compareAscDate = a.taskStartTs match {
      case Some(aTs: DateTime) =>
        b.taskStartTs match {
          case Some(bTs: DateTime) => aTs.compareTo(bTs) <= 0
          case None => false
        }
      case None => true
    }
    !compareAscDate
  }

  /**
    * Queries Mongodb stat collection to get the element processed count
    * for a specific job and a specific task
    *
    * @param job    job to find stats for
    * @param taskId task id for which to find stats for
    * @return element processed count
    */
  private def getTaskStatCount(job: BaseJob, taskId: String): Option[Long] = {
    var taskStatCount: Option[Long] = None

    getClient.foreach {
      client =>
        val collection = client.getDatabase(config.mongoDatabase()).getCollection(config.mongoStatCountCollection())
        val query = collection.find(Filters.and(Filters.equal(JOB_NAME, job.name), Filters.equal(TASK_ID, taskId))).first().toFuture()

        try {
          // FIXME: better await timeout value
          val docs = Await.result(query, Duration.Inf)

          if (docs.size > 0) {
            val doc = docs.head
            if (doc.contains(ELEMENTS_PROCESSED)) {
              doc.get(ELEMENTS_PROCESSED) match {
                case Some(l) =>
                  taskStatCount = Some(l.asInt64().longValue())
                case _ =>
              }
            }
          }
        } catch {
          case ex: java.util.concurrent.TimeoutException => log.log(Level.WARNING, "query Mongodb timeout", ex)
          case ex: Throwable =>
            log.log(Level.WARNING, "query Mongodb error", ex)
            closeClient()
        }
    }

    taskStatCount
  }

  /**
    * Determines if doc from Mongodb represents a valid Mesos task
    *
    * @param doc Mongodb document
    * @return true if valid, false otherwise
    */
  private def isValidTaskData(doc: Document): Boolean = {
    if (doc == null) {
      false
    } else {
      doc.contains(JOB_NAME) &&
        doc.contains(TASK_ID) &&
        doc.contains(TIMESTAMP) &&
        doc.contains(TASK_STATE) &&
        doc.contains(SLAVE_ID)
    }
  }

  private val terminalStates = Set(TaskState.TASK_FINISHED, TaskState.TASK_FAILED, TaskState.TASK_KILLED, TaskState.TASK_LOST).map(_.toString)

  /**
    * Parses the contents of the data doc and updates the TaskStat object
    *
    * @param taskStat task stat to be updated
    * @param doc      data document from which to update the task stat object
    * @return updated TaskStat object
    */
  private def updateTaskStat(taskStat: TaskStat, doc: Document): TaskStat = {
    val taskTimestamp = new java.util.Date(doc(TIMESTAMP).asDateTime().getValue)
    val taskState = doc(TASK_STATE).asString().getValue

    if (taskState == TaskState.TASK_RUNNING.toString) {
      taskStat.setTaskStartTs(taskTimestamp)
      taskStat.setTaskStatus(ChronosTaskStatus.Running)
    } else if (terminalStates.contains(taskState)) {
      taskStat.setTaskEndTs(taskTimestamp)
      val status = if (TaskState.TASK_FINISHED.toString == taskState) ChronosTaskStatus.Success else ChronosTaskStatus.Fail
      taskStat.setTaskStatus(status)
    }

    taskStat
  }

  /**
    * Returns a list of tasks (TaskStat) found for the specified job name
    *
    * @param job job to search for task stats
    * @return list of past and current running tasks for the job
    */
  private def getParsedTaskStatsByJob(job: BaseJob): List[TaskStat] = {
    val taskMap = mutable.Map[String, TaskStat]()

    getTaskDataByJob(job).fold {
      log.info("No row list found for jobName=%s".format(job.name))
    } {
      docsList =>
        for (doc <- docsList) {
          /*
           * Go through all the docs and construct a job history.
           * Group elements by task id
           */
          if (isValidTaskData(doc)) {
            val taskId = doc(TASK_ID).asString().getValue
            val taskStat = taskMap.getOrElseUpdate(taskId,
              new TaskStat(taskId,
                doc(JOB_NAME).asString().getValue,
                doc(SLAVE_ID).asString().getValue))
            updateTaskStat(taskStat, doc)
          } else {
            log.info("Invalid document found in Mongodb collection for jobName=%s".format(job.name))
          }
        }
    }

    taskMap.values.toList
  }

  /**
    * Returns most recent tasks by job and returns only numTasks
    *
    * @param job      job to search the tasks for
    * @param numTasks maximum number of tasks to return
    * @return returns a list of past and currently running tasks,
    *         the first element is the most recent.
    */
  def getMostRecentTaskStatsByJob(job: BaseJob, numTasks: Int): List[TaskStat] = {

    val sortedDescTaskStatList = getParsedTaskStatsByJob(job).sortWith(recentDateCompareFnc).slice(0, numTasks)

    if (job.dataProcessingJobType) {
      /*
       * Retrieve stat count for these tasks. This should be done
       * after slicing as an optimization.
       */
      sortedDescTaskStatList.foreach {
        taskStat => taskStat.numElementsProcessed = getTaskStatCount(job, taskStat.taskId)
      }
    }

    sortedDescTaskStatList
  }

  /**
    * Updates the number of elements processed by a task. This method
    * is not idempotent
    *
    * @param job                         job for which to perform the update
    * @param taskId                      task id for which to perform the update
    * @param additionalElementsProcessed number of elements to increment bt
    */
  def updateTaskProgress(job: BaseJob,
                         taskId: String,
                         additionalElementsProcessed: Long) {
    getClient.foreach {
      client =>
        val mainCollection = client.getDatabase(config.mongoDatabase()).getCollection(config.mongoCollection())
        val statCountCollection = client.getDatabase(config.mongoDatabase()).getCollection(config.mongoStatCountCollection())

        // FIXME: refactor to chained Observable
        try {
          // 1. Query main collection to make sure task exists
          val existQuery = mainCollection.find(Filters.and(Filters.equal(JOB_NAME, job.name), Filters.equal(TASK_ID, taskId))).toFuture()
          // FIXME: better await timeout value
          val docs = Await.result(existQuery, Duration.Inf)
          if (docs.size == 0) throw new IllegalArgumentException("Task id  %s not found".format(taskId))

          // 2. Update stat count collection
          val updateQuery = statCountCollection.updateOne(Filters.and(Filters.equal(JOB_NAME, job.name), Filters.equal(TASK_ID, taskId)), inc(ELEMENTS_PROCESSED, additionalElementsProcessed)).toFuture()
          Await.result(updateQuery, Duration.Inf)
        } catch {
          case ex: IllegalArgumentException => throw ex
          case ex: java.util.concurrent.TimeoutException => log.log(Level.WARNING, "query mongodb timeout", ex)
          case ex: Throwable =>
            log.log(Level.WARNING, "query mongodb error", ex)
            closeClient()
        }
    }
  }

  def asObserver: JobsObserver.Observer = JobsObserver.withName({
    case JobExpired(job, _) => updateJobState(job.name, CurrentState.idle)
    case JobRemoved(job) => removeJobState(job)
    case JobQueued(job, taskId, attempt) => jobQueued(job, taskId, attempt)
    case JobStarted(job, taskStatus, attempt) => jobStarted(job, taskStatus, attempt)
    case JobFinished(job, taskStatus, attempt) => jobFinished(job, taskStatus, attempt)
    case JobFailed(job, taskStatus, attempt) => jobFailed(job, taskStatus, attempt)
  }, getClass.getSimpleName)

  private def jobQueued(job: BaseJob, taskId: String, attempt: Int) {
    updateJobState(job.name, CurrentState.queued)
  }

  private def jobStarted(job: BaseJob, taskStatus: TaskStatus, attempt: Int) {
    updateJobState(job.name, CurrentState.running)

    var jobSchedule: Option[String] = None
    var jobParents: Option[java.util.Set[String]] = None
    job match {
      case job: ScheduleBasedJob =>
        jobSchedule = Some(job.schedule)
      case job: DependencyBasedJob =>
        jobParents = Some(job.parents.asJava)
    }
    insertToStatTable(
      id = Some(taskStatus.getTaskId.getValue),
      timestamp = Some(new java.util.Date()),
      jobName = Some(job.name),
      jobOwner = Some(job.owner),
      jobSchedule = jobSchedule,
      jobParents = jobParents,
      taskState = Some(taskStatus.getState.toString),
      slaveId = Some(taskStatus.getSlaveId.getValue),
      message = None,
      attempt = Some(attempt),
      isFailure = None)
  }

  private def jobFinished(job: BaseJob, taskStatus: TaskStatus, attempt: Int) {
    updateJobState(job.name, CurrentState.idle)

    var jobSchedule: Option[String] = None
    var jobParents: Option[java.util.Set[String]] = None
    job match {
      case job: ScheduleBasedJob =>
        jobSchedule = Some(job.schedule)
      case job: DependencyBasedJob =>
        jobParents = Some(job.parents.asJava)
    }
    insertToStatTable(
      id = Some(taskStatus.getTaskId.getValue),
      timestamp = Some(new java.util.Date()),
      jobName = Some(job.name),
      jobOwner = Some(job.owner),
      jobSchedule = jobSchedule,
      jobParents = jobParents,
      taskState = Some(taskStatus.getState.toString),
      slaveId = Some(taskStatus.getSlaveId.getValue),
      message = None,
      attempt = Some(attempt),
      isFailure = Some(false))
  }

  private def jobFailed(jobNameOrJob: Either[String, BaseJob], taskStatus: TaskStatus, attempt: Int): Unit = {
    val jobName = jobNameOrJob.fold(name => name, _.name)
    val jobSchedule = jobNameOrJob.fold(_ => None, {
      case job: ScheduleBasedJob => Some(job.schedule)
      case _ => None
    })
    val jobParents: Option[java.util.Set[String]] = jobNameOrJob.fold(_ => None, {
      case job: DependencyBasedJob => Some(job.parents.asJava)
      case _ => None
    })

    updateJobState(jobName, CurrentState.idle)
    insertToStatTable(
      id = Some(taskStatus.getTaskId.getValue),
      timestamp = Some(new java.util.Date()),
      jobName = Some(jobName),
      jobOwner = jobNameOrJob.fold(_ => None, job => Some(job.owner)),
      jobSchedule = jobSchedule,
      jobParents = jobParents,
      taskState = Some(taskStatus.getState.toString),
      slaveId = Some(taskStatus.getSlaveId.getValue),
      message = Some(taskStatus.getMessage),
      attempt = Some(attempt),
      isFailure = Some(true))
  }

  /**
    * Helper method that performs an insert statement to update the
    * job statistics (chronos) table. All arguments are surrounded
    * by options so that a subset of values can be inserted.
    */
  private def insertToStatTable(id: Option[String],
                                timestamp: Option[java.util.Date],
                                jobName: Option[String],
                                jobOwner: Option[String],
                                jobSchedule: Option[String],
                                jobParents: Option[java.util.Set[String]],
                                taskState: Option[String],
                                slaveId: Option[String],
                                message: Option[String],
                                attempt: Option[Integer],
                                isFailure: Option[Boolean]) = {

    getClient.foreach {
      client =>
        val doc = MutableDocument(TASK_ID -> id.get, JOB_NAME -> jobName.get, TIMESTAMP -> timestamp.get)

        jobOwner match {
          case Some(jo: String) => doc.update(JOB_OWNER, jo)
          case _ =>
        }
        jobSchedule match {
          case Some(js: String) => doc.update(JOB_SCHEDULE, js)
          case _ =>
        }
        jobParents match {
          case Some(jp: java.util.Set[String]) => doc.update(JOB_PARENTS, jp.asScala.toList)
          case _ =>
        }
        taskState match {
          case Some(ts: String) => doc.update(TASK_STATE, ts)
          case _ =>
        }
        slaveId match {
          case Some(s: String) => doc.update(SLAVE_ID, s)
          case _ =>
        }
        message match {
          case Some(m: String) => doc.update(MESSAGE, m)
          case _ =>
        }
        attempt match {
          case Some(a: Integer) => doc.update(ATTEMPT, BsonInt32(a))
          case _ =>
        }
        isFailure match {
          case Some(f: Boolean) => doc.update(IS_FAILURE, BsonBoolean(f))
          case _ =>
        }

        val collection: MongoCollection[MutableDocument] = client.getDatabase(config.mongoDatabase()).getCollection(config.mongoCollection())

        collection.insertOne(doc).subscribe(
          (result: Completed) => {},
          (e: Throwable) => {
            log.log(Level.WARNING, "Insert Mongodb error", e)
            closeClient()
          }
        )
    }
  }

  private def getClient: Option[MongoClient] = {
    _mongoClient match {
      case Some(s) => Some(s)
      case None =>
        config.mongoConnectionString.get match {
          case Some(c) =>
            val client = MongoClient(c)

            client.getDatabase(config.mongoDatabase()).getCollection(config.mongoCollection()).createIndex(
              compoundIndex(text(JOB_NAME), text(TASK_ID))
            ).subscribe(
              (result: String) => {},
              (e: Throwable) => log.log(Level.WARNING, "Create Mongodb index error", e)
            )

            client.getDatabase(config.mongoDatabase()).getCollection(config.mongoStatCountCollection()).createIndex(
              compoundIndex(text(JOB_NAME), text(TASK_ID))
            ).subscribe(
              (result: String) => {},
              (e: Throwable) => log.log(Level.WARNING, "Create Mongodb index error", e)
            )

            _mongoClient = Some(client)
            _mongoClient
          case None => None
        }
    }
  }

  private def closeClient(): Unit = {
    _mongoClient match {
      case Some(client) =>
        client.close()
      case _ =>
    }
    _mongoClient = None
  }
}
