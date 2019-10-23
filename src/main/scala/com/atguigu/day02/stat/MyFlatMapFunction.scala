package com.atguigu.day02.stat

import com.atguigu.day01.source.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object MyFlatMapFunction {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDataStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val sensorReadingStream: DataStream[SensorReading] = socketDataStream.map(line => {
      val splits: Array[String] = line.split(",")
      SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
    })

    sensorReadingStream.keyBy(_.id)
      .flatMap(new MyFlatMapFunction(5.0))
      .print("jump")

    env.execute("MyFlatMapFunction")
  }

}

/**
  *   温度突变则报警
  *
  * @param threshold
  */
class MyFlatMapFunction(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double, String)] {

  var lastTemp: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double, String)]): Unit = {
    val diffTemp: Double = (value.temperature - lastTemp.value()).abs
    // lastTemp.value() != null 指定不是第一次第一次进入
    if (diffTemp > threshold && lastTemp.value() != null) {
      out.collect((value.id, lastTemp.value(), value.temperature, s"temperature diff ${diffTemp}  jump too high"))
    }
    lastTemp.update(value.temperature)
  }
}
