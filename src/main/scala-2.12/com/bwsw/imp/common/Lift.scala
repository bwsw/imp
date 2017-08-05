package com.bwsw.imp.common

import scala.concurrent._
import scala.util.{Failure, Success}

/**
  * Created by ivan on 04.08.17.
  */

//
// based on
// https://stackoverflow.com/questions/29344430/scala-waiting-for-sequence-of-futures
//
object Lift {
  import ExecutionContext.Implicits.global

  private def lift[T](futures: Seq[Future[T]]) =
    futures.map(_.map { Success(_) }.recover { case t => Failure(t) })

  def waitAll[T](futures: Seq[Future[T]]) =
    Future.sequence(lift(futures))
}