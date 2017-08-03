package com.bwsw.imp.event

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 01.08.17.
  */
class Event {
  val propertyMap = mutable.Map[String, String]()
  def getProperty(key: String): String = propertyMap(key)
  def setProperty(key: String, value: String) = {
    propertyMap(key) = value
  }
}
