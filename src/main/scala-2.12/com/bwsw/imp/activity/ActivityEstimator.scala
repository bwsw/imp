package com.bwsw.imp.activity

/**
  * Created by Ivan Kudryavtsev on 06.08.17.
  */
abstract class ActivityEstimator {
  def filter(activities: Seq[Activity]): Seq[Activity]
}
