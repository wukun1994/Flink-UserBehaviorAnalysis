package com.atguigu.flink.flink

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.Map
import org.apache.flink.streaming.api.scala._


object LoginFailWithCep {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignAscendingTimestamps(_.eventTime * 1000)

    // 定义匹配模式
    val loginFailPattern = Pattern
      .begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType=="fail")
      .within(Time.seconds(2))

    //在数据流中匹配定义好的模式
    val patternStream  = CEP.pattern(loginEventStream.keyBy(_.userId),loginFailPattern)

    //.select方法传入一个pattern select function，当检测到定义好的模式序列时就会调用
    val loginFailDataStream  = patternStream.select((pattern:Map[String,Iterable[LoginEvent]])=>{
      val first = pattern.getOrElse("begin",null).iterator.next()
      val second = pattern.getOrElse("begin",null).iterator.next()
      (second.userId, second.ip, second.eventType)
    })
    loginFailDataStream.print()
    env.execute("Login Fail Detect Job")

  }

  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

}
