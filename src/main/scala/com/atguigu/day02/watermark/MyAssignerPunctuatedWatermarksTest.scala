package com.atguigu.day02.watermark

import com.atguigu.day01.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *   watermaker的生成的快慢会影响窗口window的关闭的快慢
  *   也会影响延迟数据的是否进入窗口window
  */
object MyAssignerPunctuatedWatermarksTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置event-time语义
    env.getConfig.setAutoWatermarkInterval(2000)
    env.setParallelism(1)

    val socketDataStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val sensorReadingStream: DataStream[SensorReading] = socketDataStream.map(line => {
      val splits: Array[String] = line.split(",")
      SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
    })

    val punctuatedDataStream: DataStream[SensorReading] = sensorReadingStream
      .assignTimestampsAndWatermarks(new MyAssigner2())
    punctuatedDataStream.print("punctuatedDataStream")

    punctuatedDataStream.keyBy(_.id)
        .timeWindow(Time.seconds(5))
        .minBy("temperature")
        .print("min")

    env.execute("MyAssignerPunctuatedWatermarksTest test")
  }
}

class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading] {

  val delayTime: Long = 1000 //延迟时间间隔

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    println("extractedTimestamp:"+extractedTimestamp)
    if(lastElement.id.startsWith("sensor_1")){
      new Watermark(extractedTimestamp - delayTime)
    }else{
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
   // println("previousElementTimestamp:"+previousElementTimestamp)
    element.timestamp * 1000
  }
}
