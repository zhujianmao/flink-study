package com.atguigu.transform

import com.atguigu.source.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * connect 可以将两个数据结构不一样的流合并成一个ConnectedStreams
  * map  将合成的connectStreams转换成dataStream,中间可以对流进行形式的转化
  */
object ConnectTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDataStream: DataStream[SensorReading] = env.fromCollection(Seq(SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)))

    val sourceDataStream2: DataStream[(Int, Int, Int, Int)] = env.fromCollection(Seq((1,2,3,4)))

    val connectDataStream: ConnectedStreams[SensorReading, (Int, Int, Int, Int)] = sourceDataStream.connect(sourceDataStream2)

    connectDataStream.map(sensor => sensor,{
      case (k1,k2,k3,k4) => (k1,k2*2,k3*3,k4*4)
    }).print("connect")

    env.execute("connect test")
  }

}
