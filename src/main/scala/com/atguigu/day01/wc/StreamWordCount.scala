package com.atguigu.day01.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment

    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    val port: Int = tool.getInt("port")

    val sourceDataStream: DataStream[String] = env.socketTextStream(host,port).slotSharingGroup("green")

    sourceDataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1).setParallelism(1).slotSharingGroup("blue")
      .print().setParallelism(1)

    env.execute("StreamWordCount")
  }
}
