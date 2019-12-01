package com.atguigu.gmall0624.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.bean.OrderInfo
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")

      val ssc = new StreamingContext(sparkConf,Seconds(5))

       val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    val orderDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      //电话 脱敏
      val teltuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
      val teltail: String = teltuple._2.splitAt(4)._2
      orderInfo.consignee_tel = teltuple._1 + "****" + teltail //  138****1234
      // 日期结构 改变  日期 ，小时

      val dateAndTimeArray: Array[String] = orderInfo.create_time.split(" ")

      orderInfo.create_date = dateAndTimeArray(0)
      orderInfo.create_hour = dateAndTimeArray(1).split(":")(0)

      // 订单上增加一个字段   该订单是否是该用户首次下单
      //维护一个 状态   该用户是否过单的状态  // redis 可以   // mysql 可以
      orderInfo
    }


    orderDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0624_ORDER_INFO", Seq( "ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration,Some("hadoop1,hadoop2,hadoop3:2181"))

    }


    ssc.start()
    ssc.awaitTermination()






  }

}
