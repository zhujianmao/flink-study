package com.atguigu.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
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

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()

    kafkaSourceDataStream.addSink(new RedisSink(config,new MyRedisMapper()))

    env.execute("kafka source test")
  }
}

class MyRedisMapper() extends RedisMapper[String]{


  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"redis")
  }

  override def getKeyFromData(data: String): String = {
    println("key:"+data)
    data
  }

  override def getValueFromData(data: String): String = {
    println("value:"+data)
    data.size.toString
  }
}
