package com.bigdata.hisign.sparkstreaming2

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object StreamingConsumerBySingletion {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("streaming-kafka")
      .setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
//    通过单例的方式全局创建一个spark会话
    val spark = SparkSessionSingleton.getInstance(sparkConf)
    val sc = spark.sparkContext
    //    把这个spark会话广播出去，后面在每个rdd里面获取使用
    val ssc = new StreamingContext(sc, Seconds(1))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "cn75.hadoop.hisign:9092,cn76.hadoop.hisign:9092,cn77.hadoop.hisign:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "hisign01",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = Set("test01")

    val data: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topic,kafkaParams)
    )

    data.foreachRDD(rdd=> {
      println(rdd.count())
      //      通过广播变量获取共享的spark会话
      val tmp_spark = SparkSessionSingleton.getInstance(sparkConf)
      //      有消息时才进行处理
      if (!rdd.isEmpty()) {
        //        对获取的rdd数据进行封装
        val rddRow: RDD[Row] = rdd.map(line => {
          val tmp01: String = line.value()
          //          json字符串转为json对象
          val jSONObject: JSONObject = JSON.parseObject(line.value())
          val id = jSONObject.get("id").toString
          Row(id)
        })
        val structfields = List(
          StructField("id", StringType, true)
        )
        val schema = StructType(structfields)
        //        创建一个数据框对象
        val df01 = tmp_spark.createDataFrame(rddRow,schema)
        //        创建一个临时表
        df01.createOrReplaceTempView("sr01")
        //        把临时sr01中的数据写入目标表
        tmp_spark.sql("insert into table xzxtods.tb06 select * from sr01")
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
