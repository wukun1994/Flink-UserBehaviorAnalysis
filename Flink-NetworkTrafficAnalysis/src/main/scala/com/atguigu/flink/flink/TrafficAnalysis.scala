package com.atguigu.flink.flink

import java.text.SimpleDateFormat

import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object TrafficAnalysis {

  /**
    * 实时流量统计
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile("D:\\idea\\workspace\\Flink-UserBehaviorAnalysis\\Flink-NetworkTrafficAnalysis\\src\\main\\resources\\apachetest.log")
      .map(line=>{
           val linearray = line.split(" ")
           val simpleDateFormat  = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
           val timestamp  = simpleDateFormat.parse(linearray(3)).getTime
           ApacheLogEvent(linearray(0), linearray(2), timestamp, linearray(5), linearray(6))
           })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)){
           override def extractTimestamp(t: ApacheLogEvent): Long = {
              t.eventTime
        }
      })
      .keyBy("url")
      .timeWindow(Time.minutes(10),Time.minutes(5))
      .aggregate(new CountAgg,new WindowResultFunction)
      .keyBy(1)
      .process(new TopNHotUrls(5))
      .print()
    env.execute("Traffic Analysis Job")

  }

  class CountAgg extends AggregateFunction[ApacheLogEvent,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFunction extends WindowFunction[Long,UrlViewCount,Tuple,TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], collector: Collector[UrlViewCount]): Unit = {
      val url = key.asInstanceOf[org.apache.flink.api.java.tuple.Tuple1[String]].f0
      val count: Long = input.iterator.next
      collector.collect(UrlViewCount(url, window.getEnd, count))
    }
  }

  case class TopNHotUrls(topSize:Int) extends KeyedProcessFunction[Tuple,UrlViewCount,String]{

    private var urlState:ListState[UrlViewCount]=_

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val urlStateDesc = new ListStateDescriptor[UrlViewCount]("urlState-state",classOf[UrlViewCount])
      urlState = getRuntimeContext.getListState(urlStateDesc)
    }
    override def processElement(input: UrlViewCount, context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 每条数据都保存到状态中
      urlState.add(input)
      context.timerService.registerEventTimeTimer(input.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 获取收到的所有URL访问量
      val allUrlViews:ListBuffer[UrlViewCount] = ListBuffer()
      import  scala.collection.JavaConversions._
      for (urlView <- urlState.get){
        allUrlViews += urlView
      }
      // 提前清除状态中的数据，释放空间
      urlState.clear()
      // 按照访问量从大到小排序
      val sortedUrlViews  = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result.append("No").append(i+1).append(":")
          .append("  URL=").append(currentUrlView.url)
          .append("  流量=").append(currentUrlView.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }

  case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

  case class UrlViewCount(url:String,windowEnd:Long,count:Long)
}
