package com.bwsw.imp.activity

/**
  * Created by Ivan Kudryavtsev on 06.08.17.
  */
class PassThroughActivityEstimator extends ActivityEstimator {
  override def filter(activities: Seq[Activity]): Seq[Activity] = activities
}
