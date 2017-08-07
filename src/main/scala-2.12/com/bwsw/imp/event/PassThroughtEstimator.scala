package com.bwsw.imp.event
import com.bwsw.imp.activity.Activity

/**
  * Created by Ivan Kudryavtsev on 06.08.17.
  */
class PassThroughtEstimator extends Estimator {
  override def filter(activities: Seq[Activity]): Seq[Activity] = activities
}
