package com.atguigu.day02.processFunctionApi

import com.atguigu.day01.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDataStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val sensorReadingStream: DataStream[SensorReading] = socketDataStream.map(line => {
      val splits: Array[String] = line.split(",")
      SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
    })

    val timeDataStream: DataStream[(String, Double, String)] = sensorReadingStream
      .process(new MySideOut())
    timeDataStream.print("healthy")
    timeDataStream.getSideOutput(new OutputTag[String]("freezing-alarms")).print("freezing")
    env.execute("SideOutProcessFunctionTest test")
  }

}

/**
  * 将温度值低于32F的温度输出到side output
  */
class MySideOut() extends ProcessFunction[SensorReading, (String, Double, String)] {
  // 定义一个侧输出标签
  lazy val freezingAlarmOutput: OutputTag[String] =
    new OutputTag[String]("freezing-alarms")

  override def processElement(value: SensorReading,
                              ctx: ProcessFunction[SensorReading, (String, Double, String)]#Context,
                              out: Collector[(String, Double, String)]): Unit = {
    if (value.temperature < 32) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id}")
    } else {
      out.collect((value.id, value.temperature, "healthy"))
    }
  }
}