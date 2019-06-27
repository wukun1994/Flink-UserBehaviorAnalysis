package com.atguigu.flink

object LoginFail {

  def main(args: Array[String]): Unit = {


  }

  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
}
