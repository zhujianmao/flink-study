package com.atguigu.sink

import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object ElasticSearchSinkTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers", "hadoop102:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    val kafkaSourceDataStream: DataStream[String] =
      env.addSource(new FlinkKafkaConsumer011[String]("source", new SimpleStringSchema(), props))

    val hosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("hadoop102", 9200))
    val esBuilder: ElasticsearchSink.Builder[String] = new ElasticsearchSink.Builder[String](hosts, new ElasticsearchSinkFunction[String] {
      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

        val map:util.Map[String,String] = new  util.HashMap[String,String]()
        map.put("data",t)

        val indexRequest: IndexRequest = Requests.indexRequest("sensor")
          .index("sensor")
          .`type`("_doc")
          .source(map)

        requestIndexer.add(indexRequest)
        println(t+"-success")
      }
    })
    kafkaSourceDataStream.addSink(esBuilder.build())

    env.execute("elasticSearch test")
  }

}
