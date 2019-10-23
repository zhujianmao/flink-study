package com.atguigu.day02.processFunctionApi

import com.atguigu.day01.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDataStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val sensorReadingStream: DataStream[SensorReading] = socketDataStream.map(line => {
      val splits: Array[String] = line.split(",")
      SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
    })

    val timeDataStream: DataStream[String] = sensorReadingStream.keyBy(_.id)
      .process(new MyKeyProcessFunction)

    timeDataStream.print("healthy")
    timeDataStream.getSideOutput(new OutputTag[String]("warning")).print("warning")

    env.execute("KeyedProcessFunctionTest test")
  }

}

/**
  * 监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连续上升，则报警。
  *
  *   正常的流
  *
  */
class MyKeyProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {
  // 保存上一个传感器温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 保存注册的定时器的时间戳
  lazy val registerSensorTime: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("registerSensorTime", classOf[Long]))

  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    //当前温度
    val temperatcure: Double = value.temperature
    //上一次温度
    val lastTemperature: Double = lastTemp.value()
    // 注册定时器的时间戳
    val curTime: Long = registerSensorTime.value()
    //更新上一次传感器的温度
    lastTemp.update(temperatcure)

    if (temperatcure > lastTemperature && curTime == 0) {
      val ts: Long = ctx.timerService().currentProcessingTime() + 10 * 1000
      ctx.timerService().registerProcessingTimeTimer(ts)
      registerSensorTime.update(ts)
    } else if (temperatcure < lastTemperature) {
      ctx.timerService().deleteProcessingTimeTimer(curTime)
      registerSensorTime.clear()
      out.collect("sensor "+ctx.getCurrentKey+ " 的传感器温度值恢复正常!")
    }

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    ctx.output(new OutputTag[String]("warning"),"sensor " + ctx.getCurrentKey + "的传感器温度值已经连续10s上升了。")
    registerSensorTime.clear()
  }
}