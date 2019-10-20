package com.atguigu.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * 从文件读取数据
  */
object FileSourceTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val path: String = "E:\\code\\idea\\Flink-Tutorial\\src\\main\\resources\\sensor,.txt"
    val fileSourceDataStream: DataStream[String] = env.readTextFile(path)

    fileSourceDataStream.print("file")
    env.execute("file source test")
  }
}
