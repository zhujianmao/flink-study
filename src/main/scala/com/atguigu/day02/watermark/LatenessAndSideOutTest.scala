package com.atguigu.day02.watermark

import com.atguigu.day01.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *  窗口长度为5s,watermaker延迟为5s
  *
  *   忍受的延迟关窗时间为5s
  *
  *   最终延迟的数据输出到侧输出流,最后进行批处理
  *
  */
object LatenessAndSideOutTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置event-time语义
    env.getConfig.setAutoWatermarkInterval(200)
    env.setParallelism(1)

    val socketDataStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val sensorReadingStream: DataStream[SensorReading] = socketDataStream.map(line => {
      val splits: Array[String] = line.split(",")
      SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
    })

    val periodDataStream: DataStream[SensorReading] = sensorReadingStream
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      }) //设置watermaker

    val outputTag: OutputTag[SensorReading] = new OutputTag[SensorReading]("sideOutputLateData")
    val mainDataStream: DataStream[SensorReading] = periodDataStream.keyBy(_.id)
      .timeWindow(Time.seconds(5)) //设置TumblingEventTimeWindow
      .allowedLateness(Time.seconds(5))
      .sideOutputLateData(outputTag)
      .minBy("temperature")

    mainDataStream.print("main")
    mainDataStream.getSideOutput(outputTag).print("side")

    env.execute("LatenessAndSideOutTest test")
  }
}
