package com.bwsw.imp.message

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 03.08.17.
  */
abstract class Message extends Serializable {

  val uuid = java.util.UUID.randomUUID()

  //todo: Possible using immutable Map
  val propertyMap = mutable.Map[String, String]()

  //todo: Can be simpler => propertyMap.get(key) -- Option[String]
  def getProperty(key: String): Option[String] = {
    if (propertyMap.contains(key))
      Some(propertyMap(key))
    else
      None
  }

  //todo: what if element is not found?
  def setProperty(key: String, value: String) = {
    propertyMap(key) = value
  }

  def hash: String = uuid.toString
}
