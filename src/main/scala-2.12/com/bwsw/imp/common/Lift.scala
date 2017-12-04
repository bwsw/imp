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

  //todo: should be better, if you add to signature of method ExecutionContext as implicit param.
  //ExecutionContext.global can be used in tests (only - it's my opinion)
  private def lift[T](futures: Seq[Future[T]]) =
    futures.map(_.map { Success(_) }.recover { case t => Failure(t) })

  //todo: need to result type in signature of method
  def waitAll[T](futures: Seq[Future[T]]) =
    Future.sequence(lift(futures))
}