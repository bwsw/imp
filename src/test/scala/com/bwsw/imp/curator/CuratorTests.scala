package com.bwsw.imp.curator

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 31.07.17.
  */

class CuratorTests extends FlatSpec with Matchers with BeforeAndAfterAll {
  val ZOOKEEPER_PORT = 21810
  var testingServer = new TestingServer(ZOOKEEPER_PORT)
  implicit var curator: CuratorFramework = _

  override def beforeAll() = {
    testingServer.start()
    curator = CuratorFrameworkFactory.builder()
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("tests")
      .connectString(s"127.0.0.1:$ZOOKEEPER_PORT").build()
    curator.start()
  }

  it must "do nothing" in {}

  override def afterAll() = {
    curator.close()
    testingServer.stop()
  }

}