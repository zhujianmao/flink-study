package com.atguigu.day02.window

import com.atguigu.day01.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SlidingTimeWindow {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDataStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val sensorReadingStream: DataStream[SensorReading] = socketDataStream.map(line => {
      val splits: Array[String] = line.split(",")
      SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
    })
    sensorReadingStream.keyBy(_.id)
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .minBy("temperature")
      .print("min")

    env.execute("sliding process time window")
  }
}
