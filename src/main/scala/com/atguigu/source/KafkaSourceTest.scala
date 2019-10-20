package com.atguigu.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * 从kafka里读取数据
  */
object KafkaSourceTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers","192.168.6.102:9092")
    props.setProperty("group.id", "kafka-source")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    val kafkaSourceDataStream: DataStream[String] =
        env.addSource(new FlinkKafkaConsumer011[String]("test",new SimpleStringSchema(),props))

    kafkaSourceDataStream.print()

    env.execute("kafka source test")
  }

}
