package com.atguigu.wc

import org.apache.flink.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val path: String = "E:\\code\\idea\\Flink-Tutorial\\src\\main\\resources\\wc.txt"

    val sourceDataSource: DataSet[String] = env.readTextFile(path)

    sourceDataSource.flatMap(_.split("\\W+"))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()

  }
}
