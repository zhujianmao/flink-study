package com.atguigu.source

import org.apache.flink.streaming.api.scala._

/**
  * 从集合里面读取数据
  */
object CollectionSourceTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDataStream: DataStream[SensorReading] = env.fromCollection(Seq(SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)))
    sourceDataStream.print()

    env.execute("collection source test")
  }
}

case class SensorReading(id:String,timestamp:Long,temperature:Double)
