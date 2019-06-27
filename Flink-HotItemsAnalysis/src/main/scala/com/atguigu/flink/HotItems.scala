package com.atguigu.flink


import java.util.Properties

import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {

  /**
    *  计算最热门Top N商品
    * @param args
    */

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")



    // 创建一个 StreamExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序，我配置全局的并发为1，这里改变并发对结果正确性没有影响
    env.setParallelism(1)
    val stream: DataStream[String] = env
      // 以window下为例，需替换成自己的路径

      .readTextFile("D:\\idea\\workspace\\Flink-UserBehaviorAnalysis\\Flink-HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      //替换数据来源为kafka
      //  .addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      //需要加隐式转换import org.apache.flink.streaming.api.scala._
      .map(line => {
        val linearray: Array[String] = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      })
      // 指定时间戳和watermark
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior=="pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .aggregate(new CountAgg(),new WindowResultFunction())
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))
    stream.print
    env.execute("Hot Items Job")

  }

  class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFunction extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, iterable: Iterable[Long], collector: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.asInstanceOf[org.apache.flink.api.java.tuple.Tuple1[Long]].f0
      val count: Long = iterable.iterator.next
      collector.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

  class TopNHotItems(topSize:Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{

    private var itemState:ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      //命名状态变量的名字和状态变量的类型
      val itemsStateDesc: ListStateDescriptor[ItemViewCount] = new ListStateDescriptor[ItemViewCount]("itemState-state",classOf[ItemViewCount])
      itemState=getRuntimeContext.getListState(itemsStateDesc)
    }

    override def processElement(input: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      //每条数据都保存在状态中
      itemState.add(input)
      //注册WindowEnd+1 的EventTime Timer，当触发时，说明收齐了数据WindowEnd窗口的所有商品数据
      //也就是当程序看到WindowEnd + 1的水位线watermark时,触发onTime回调函数
      context.timerService.registerEventTimeTimer(input.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //获取收到的所有商品点击量
      val allItems:ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for(item <- itemState.get){
        allItems += item
      }
    //提前清除状态中的数据，释放空间
      itemState.clear()
      //按照点击从大到小排序
      val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      //将排名信息格式化成String，便于打印
      val result: StringBuilder = new StringBuilder
      result.append("==============================\n")
      result.append("时间: ").append(new Timestamp(timestamp -1)).append("\n")

      for(i <- sortedItems.indices){
        val currentItem: ItemViewCount = sortedItems(i)
        result
          .append("No")
          .append(i+1)
          .append(":")
          .append("  商品ID=")
          .append(currentItem.itemId)
          .append("  浏览量=")
          .append(currentItem.count)
          .append("\n")
      }

      result.append("===========================\n\n")
      //控制输出频率,模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)


    }
  }
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

  case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

}
