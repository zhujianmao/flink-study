package com.atguigu.day02.watermark

import com.atguigu.day01.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object MyAssignerTimestampsAndWatermarksTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置event-time语义
    env.getConfig.setAutoWatermarkInterval(2000)
    //env.setParallelism(1)
    val socketDataStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val sensorReadingStream: DataStream[SensorReading] = socketDataStream.map(line => {
      val splits: Array[String] = line.split(",")
      SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
    })

    val periodDataStream: DataStream[SensorReading] = sensorReadingStream
      .assignTimestampsAndWatermarks(new MyAssigner())
    periodDataStream.print("periodDataStream")

    periodDataStream.keyBy(_.id)
      .timeWindow(Time.seconds(15))
      .minBy("temperature")
      .print("min")



    env.execute("MyAssignerPunctuatedWatermarksTest test")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {

  val delayTime: Long = 1000 //延迟时间间隔
  var maxTime: Long = 0 // 最大的时间戳
  /**
    *  env.getConfig.setAutoWatermarkInterval(2000)
    * 每隔一段时间调用,不是每条输入都调用
    *
    * @return
    */
  override def getCurrentWatermark: Watermark = {
    //println("MyAssigner:"+System.currentTimeMillis())
    new Watermark(maxTime - delayTime)
  }

  /**
    * 每条记录都会调用该方法,更新 maxTime
    *
    * @param element
    * @param previousElementTimestamp
    * @return
    */
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    println("extractTimestamp" + element)
    maxTime = maxTime.max(element.timestamp)
    element.timestamp * 1000
  }
}
