package com.bwsw.imp.activity

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 09.08.17.
  */
class ActivityTests extends FlatSpec with Matchers {
  it should "behave as expected" in {
    val activity = new Activity {
      override def activate(e: Environment): Seq[Activity] = Nil
    }

    activity.hash.matches("^([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})-0$") shouldBe true
    activity.activateIt(new Environment) shouldBe Nil
    activity.hash.matches("^([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})-1$") shouldBe true

  }
}
