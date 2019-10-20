package com.atguigu.sink



import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
  * kafka Sink
  * 往Kafka里面写数据
  */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    val kafkaSourceDataStream: DataStream[String] =
      env.addSource(new FlinkKafkaConsumer011[String]("source",new SimpleStringSchema(),props))
    kafkaSourceDataStream.print("kafka")
    kafkaSourceDataStream.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092","sink",new SimpleStringSchema()))

    env.execute("kafka source test")
  }

}
