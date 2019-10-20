package com.atguigu.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object MySqlSink {
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
    val peopleDataStream: DataStream[People] = kafkaSourceDataStream.map(line => {
      val splits: Array[String] = line.split(",")
      People(splits(0).toInt, splits(1), splits(2).toInt)
    })

    peopleDataStream.addSink(new MySqlSink()).setParallelism(1)
    env.execute("mysql sink")
  }

}

class MySqlSink() extends RichSinkFunction[People]{
  var conn: Connection = _
  var insertPre : PreparedStatement = _
  var updatePre : PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456")
    insertPre = conn.prepareStatement("insert into people(id,name,age) values(?,?,?)")
    updatePre = conn.prepareStatement("update people set name = ? , age = ? where id = ? ")
  }

  override def invoke(value: People): Unit = {
    updatePre.setString(1,value.name)
    updatePre.setInt(2,value.age)
    updatePre.setInt(3,value.id)
    updatePre.execute()
    if(updatePre.getUpdateCount == 0){
      insertPre.setInt(1,value.id)
      insertPre.setString(2,value.name)
      insertPre.setInt(3,value.age)
      insertPre.execute()
    }
  }

  override def close(): Unit = {
    insertPre.close()
    updatePre.close()
    conn.close()
  }
}

case class People(id:Int,name:String,age:Int)