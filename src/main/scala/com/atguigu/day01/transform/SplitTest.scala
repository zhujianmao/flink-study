package com.atguigu.day01.transform

import com.atguigu.day01.source.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * connect 将两个流打上标签
  * select  根据标签去查询出对应的流的数据
  */
object SplitTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDataStream: DataStream[SensorReading] = env.fromCollection(Seq(SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)))

    val splitDataStream: SplitStream[SensorReading] = sourceDataStream.split(sensor => if(sensor.temperature > 30) Seq("high") else Seq("low"))

    splitDataStream.select("high").print("high")
    splitDataStream.select("low").print("low")
    splitDataStream.select("low","high").print("all")

    env.execute("split test")
  }

}
