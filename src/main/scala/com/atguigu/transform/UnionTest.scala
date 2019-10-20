package com.atguigu.transform

import com.atguigu.source.SensorReading
import org.apache.flink.streaming.api.scala._

object UnionTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDataStream: DataStream[SensorReading] = env.fromCollection(Seq(SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)))

    val splitDataStream: SplitStream[SensorReading] = sourceDataStream.split(sensor => if (sensor.temperature > 30) Seq("high") else Seq("low"))

    val highDataStream: DataStream[SensorReading] = splitDataStream.select("high")
    val lowDataStream: DataStream[SensorReading] = splitDataStream.select("low")

    highDataStream.union(lowDataStream).print("union")

    env.execute("union test")

  }
}
