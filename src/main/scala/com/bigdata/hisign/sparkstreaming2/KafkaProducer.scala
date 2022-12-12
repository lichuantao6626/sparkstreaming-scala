package com.bigdata.hisign.sparkstreaming2

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties, UUID}

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}


object KafkaProducer {
  def main(args: Array[String]): Unit = {
//    val topic_name=args(0)
//    val count:Int = Integer.parseInt(args(1))
    val topic_name="test01"
    val count:Int = 10000
    val prop = new Properties
    // 指定请求的kafka集群列表
//    prop.put("bootstrap.servers", "ambari01.hisign.com:6667,ambari02.hisign.com:6667,ambari03.hisign.com:6667")// 指定响应方式
//    prop.put("bootstrap.servers", "192.168.11.212:9092,192.168.11.214:9092,192.168.11.215:9092")// 指定响应方式
//    prop.put("bootstrap.servers", "192.168.43.207:9092,192.168.43.208:9092,192.168.43.209:9092")
//    湖北单独kafka集群
//    prop.put("bootstrap.servers", "xzxt01.hubei.kafka:9092,xzxt02.hubei.kafka:9092")
//    公司测试环境
    prop.put("bootstrap.servers", "cn75.hadoop.hisign:9092,cn76.hadoop.hisign:9092,cn77.hadoop.hisign:9092")
    //prop.put("acks", "0")
    prop.put("acks", "1")
    // 请求失败重试次数
    //prop.put("retries", "3")
    // 指定key的序列化方式, key是用于存放数据对应的offset
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 指定value的序列化方式
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 配置超时时间
    prop.put("request.timeout.ms", "60000")
    //prop.put("batch.size", "16384")
    //prop.put("linger.ms", "1")
    //prop.put("buffer.memory", "33554432")

    // 得到生产者的实例
    val producer = new KafkaProducer[String, String](prop)

    // 模拟一些数据并发送给kafka
    for (i <- 0 to count) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val rksj: String = sdf.format(new Date())
      val hm: util.HashMap[String, String] = new util.HashMap[String,String]()
      hm.put("id",s"${UUID.randomUUID().toString}")
//      hm.put("NAME",s"zhangsan:$i")
//      hm.put("rksj",s"$rksj")
      val json_msg: String = JSON.toJSON(hm).toString
//      println(json_msg)
      // 得到返回值
      val rmd: RecordMetadata = producer.send(new ProducerRecord[String, String](topic_name, json_msg)).get()
//      println(rmd.toString)
      producer.flush()
      println("发送完一条消息")
      Thread.sleep(1000)
    }
    producer.flush()
    producer.close()
  }

}
