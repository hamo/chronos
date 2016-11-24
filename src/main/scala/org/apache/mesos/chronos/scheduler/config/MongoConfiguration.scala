package org.apache.mesos.chronos.scheduler.config

import org.rogach.scallop.ScallopConf

trait MongoConfiguration extends ScallopConf {

  lazy val mongoConnectionString = opt[String]("mongo_connection_string",
    descr = "mongo connection string",
    default = None)

  lazy val mongoDatabase = opt[String]("mongo_database",
    descr = "database to use for Mongodb",
    default = None)

  lazy val mongoCollection = opt[String]("mongo_collection",
    descr = "Collection to use for Mongodb",
    default = Some("chronos"))

  lazy val mongoStatCountCollection = opt[String]("mongo_stat_count_collection",
    descr = "Collection to track stat counts in Mongodb",
    default = Some("chronos_stat_count"))

  lazy val jobHistoryLimit = opt[Int]("job_history_limit",
    descr = "Number of past job executions to show in history view",
    default = Some(100))
}
