package com.atguigu.source

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

/**
  * 自定义Source
  */
object MySource {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val mySourceDataStream = env.addSource(new MySource())

    mySourceDataStream.print("my source")

    env.execute("my source")

  }
}

class MySource() extends SourceFunction[SensorReading]{
  //运行的标志位
  var running: Boolean = true
  // 随机生成十个温度传感器
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random: Random = new Random()
    val sensors: Seq[(String, Double)] = 1.to(10).map(i => ("senor-"+i,60+random.nextGaussian()*20))
    while (running){
        val currentTime = System.currentTimeMillis()
      sensors.foreach{
        case (sensorId,temperature) => ctx.collect(SensorReading(sensorId,currentTime,temperature))
      }
    }
    Thread.sleep(1000)
  }
  //调用该方法会取消运行
  override def cancel(): Unit = running = false
}
