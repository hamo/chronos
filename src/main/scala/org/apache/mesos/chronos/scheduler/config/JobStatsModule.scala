package org.apache.mesos.chronos.scheduler.config

import com.google.inject.{AbstractModule, Provides, Scopes, Singleton}
import org.apache.mesos.chronos.scheduler.jobs.stats.JobStats

class JobStatsModule(config: MongoConfiguration) extends AbstractModule {
  def configure() {
    bind(classOf[JobStats]).in(Scopes.SINGLETON)
  }

  @Provides
  @Singleton
  def provideConfig() = {
    config
  }
}
