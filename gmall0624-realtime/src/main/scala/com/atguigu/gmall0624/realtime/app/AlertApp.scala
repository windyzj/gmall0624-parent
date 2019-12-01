package com.atguigu.gmall0624.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0624.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.control.Breaks._

object AlertApp {


//  1  同一设备  groupby  mid
//
//  2  5分钟内  开窗口 （ 窗口大小，滑动步长）   窗口大小 体现取数的范围  滑动步长 统计频率
//
//  3  三次及以上用不同账号登录并领取优惠劵  得到窗口内 mid的操作行为集合  判断集合是否符合预警规则
//
//  4  并且在登录到领劵过程中没有浏览商品。
//
//  5 写日志     ES
//
//  6 同一设备，每分钟只记录一次预警。  时间单位内 去重
//
//  去重的手段
//  1  利用redis等第三方储存  来保存 状态         保留首次
//
//  2  利用最终存储容器的幂等性进行去重           保留末次
//  es     PUT   id=  mid+分钟级时间戳
//  hbase  PUT
//
//  3 开大窗口

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alert_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)


    //  基本转换
    val eventInfoDStream: DStream[EventInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])

      val date = new Date(eventInfo.ts)
      val formattor = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateString: String = formattor.format(date)
      val dateArr: Array[String] = dateString.split(" ")
      eventInfo.logDate = dateArr(0)
      eventInfo.logHour = dateArr(1)
      eventInfo
    }


    //    5分钟内  开窗口
    val windowDStream: DStream[EventInfo] = eventInfoDStream.window(Seconds(300),Seconds(5))


    //    分组
    val groupbyMidDStream: DStream[(String, Iterable[EventInfo])] = windowDStream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()


    groupbyMidDStream.cache()

    val alertInfoDStream: DStream[(Boolean, AlertInfo)] = groupbyMidDStream.map { case (mid, eventInfoItr) =>
      // a 三次及以上用不同账号登录并领取优惠劵  得到窗口内 mid的操作行为集合  判断集合是否符合预警规则
      // b 并且在登录到领劵过程中没有浏览商品。
      var isAlert = false
      var isClickItem = false
      val eventList = new util.ArrayList[String]()
      val uidSet = new util.HashSet[String]()
      val itemSet = new util.HashSet[String]()

      breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          if (eventInfo.evid == "coupon") {
            uidSet.add(eventInfo.uid)
            itemSet.add(eventInfo.itemid)
          }
          if (eventInfo.evid == "clickItem") {
            isClickItem = true
            break
          }
          eventList.add(eventInfo.evid)
        }
      )
      if (uidSet.size > 3 && isClickItem == false) { // 购物车登录账号>3个  且 未点击商品信息
        isAlert = true
      }

      (isAlert, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
    }

    alertInfoDStream.foreachRDD(rdd=>
      println(rdd.collect().mkString("\n"))
    )

    val filteredAlertInfo: DStream[(Boolean, AlertInfo)] = alertInfoDStream.filter(_._1)

    //同一设备，每分钟只记录一次预警。  时间单位内 去重
    // 利用es 的id 进行去重   相同mid每分钟的ID是相同的  mid+分钟的时间戳

    val alertInfoWithIdDstream: DStream[(String, AlertInfo)] = filteredAlertInfo.map { case (flag, alertInfo) =>
      val minute = alertInfo.ts / 60000
      val id: String = alertInfo.mid + "_" + minute
      (id, alertInfo)
    }
    alertInfoWithIdDstream.foreachRDD{rdd=>
      //以分区为单位 进行批量插入ES
      rdd.foreachPartition{ alertWithIdItr=>
        val alertWithIdList: List[(String, AlertInfo)] = alertWithIdItr.toList
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_ALERT, alertWithIdList)
      }

    }

    ssc.start()
    ssc.awaitTermination()


  }

}
