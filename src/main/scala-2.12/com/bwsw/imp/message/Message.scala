package com.bwsw.imp.message

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
abstract class Message extends Serializable {

  val uuid = java.util.UUID.randomUUID()

  val propertyMap = mutable.Map[String, String]()

  def getProperty(key: String): Option[String] = {
    if (propertyMap.contains(key))
      Some(propertyMap(key))
    else
      None
  }

  def setProperty(key: String, value: String) = {
    propertyMap(key) = value
  }

  def hash: String = uuid.toString
}
