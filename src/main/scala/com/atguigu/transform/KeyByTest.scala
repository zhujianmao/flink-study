package com.atguigu.transform

import com.atguigu.source.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._


/**
  * 测试基于keyBy的滚动聚合算子
  */
object KeyByTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sourceDataStream: DataStream[SensorReading] = env.fromCollection(Seq(SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444),
      SensorReading("sensor_1", 1547718199, 35.9)))

    sourceDataStream.keyBy(_.id)
      .max("temperature")
      .print("maxTest")

    sourceDataStream.keyBy("id")
      .reduce(new ReduceFunction[SensorReading] {
        override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
          SensorReading(value1.id, value1.timestamp + 1, value2.temperature + 10)
        }
      }).print("reduceTest")
    env.execute("KeyBy test")

  }
}
