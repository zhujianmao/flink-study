package com.atguigu.day02.stat

import com.atguigu.day01.source.SensorReading
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.operators.{CheckpointCommitter, GenericWriteAheadSink}

object FlatMapStatTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val socketDataStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val sensorReadingStream: DataStream[SensorReading] = socketDataStream.map(line => {
      val splits: Array[String] = line.split(",")
      SensorReading(splits(0), splits(1).toLong, splits(2).toDouble)
    })

    sensorReadingStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double, String),Double]{
      case (in:SensorReading,None) => (Set.empty,Some(in.temperature))
      case (in:SensorReading,Some(lastTemp:Double)) =>
            val diffTemp: Double = (in.temperature - lastTemp).abs
            if(diffTemp > 10)
              (Set((in.id,lastTemp,in.temperature,s"jump temperature ${diffTemp} two high")),Some(in.temperature))
            else
              (Set.empty,Some(in.temperature))
    }
      .print("jump")

    env.execute("FlatMapStatTest")


  }

}
