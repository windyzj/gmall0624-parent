package com.atguigu.gmall0624.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0624.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import org.json4s.native.Serialization

object SaleApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("sale_app")

      val ssc = new StreamingContext(sparkConf,Seconds(5))

      val orderInfoInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)
      val orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL,ssc)


      val orderDstream: DStream[OrderInfo] = orderInfoInputDstream.map { record =>
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

    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputDstream.map(_.value()).map(JSON.parseObject(_,classOf[OrderDetail]))


    val orderInfoWithIdDstream: DStream[(String, OrderInfo)] = orderDstream.map(orderInfo=>(orderInfo.id,orderInfo))

    val orderDetailWithOidDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

    val orderFullJoinedDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithIdDstream.fullOuterJoin(orderDetailWithOidDstream)

   //双流join
    val saleDetailDstream: DStream[SaleDetail] = orderFullJoinedDstream.flatMap { case (orderId, (orderInfoOption, orderDetailOption)) =>

      implicit val formats = org.json4s.DefaultFormats
      val saleDetailListBuffer = ListBuffer[SaleDetail]()
      val jedis: Jedis = RedisUtil.getJedisClient
      if (orderInfoOption != None) {
        // 1 组合saleDetail
        val orderInfo: OrderInfo = orderInfoOption.get
        if (orderDetailOption != None) {
          val orderDetail: OrderDetail = orderDetailOption.get
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailListBuffer += saleDetail
        }
        //2 写缓存
        // redis key设计   type  string       key   order_info:[order_id]   value  order_info_json
        val orderInfoKey = "order_info:" + orderInfo.id
        //val orderInfoJsonString: String = JSON.toJSONString(orderInfo)
        val orderInfoJson: String = Serialization.write(orderInfo)
        jedis.setex(orderInfoKey, 600, orderInfoJson)

        //3 读缓存 orderDetail 组合成saleDetail
        //根据order_id  查询对应detail
        val orderDetailKey = "order_detail:" + orderInfo.id
        val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
        if (orderDetailSet != null && orderDetailSet.size() > 0) {
          import scala.collection.JavaConversions._
          for (orderDetailJsonString <- orderDetailSet) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonString, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailListBuffer += saleDetail
          }
        }

      } else {
        val orderDetail: OrderDetail = orderDetailOption.get
        //1 写缓存  //相同orderId的detail保存为一个集合
        // redis key 设计 type :set   key  order_detail:[order_id] value  多个order_detail_json
        val orderDetailKey = "order_detail:" + orderDetail.order_id

        val orderDetailJson: String = Serialization.write(orderDetail)
        // JSON.toJSONString(orderDetail,new SerializeConfig(true))
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 600)

        //2 读缓存
        //用orderId 查询orderInfo的缓存（String）  得到orderInfo 对象
        val orderInfoKey = "order_info:" + orderDetail.order_id
        val orderInfoJsonString: String = jedis.get(orderInfoKey)
        if (orderInfoJsonString != null && orderInfoJsonString.size > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJsonString, classOf[OrderInfo])
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailListBuffer += saleDetail
        }

      }
      jedis.close()

      saleDetailListBuffer

    }
    saleDetailDstream.print()
    //关联维表
    val saleDetailFinalDstream: DStream[SaleDetail] = saleDetailDstream.mapPartitions { saleDetailItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val saleDetailListBuffer: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
      for (saleDetail <- saleDetailItr) {
        val userKey = "user_info:" + saleDetail.user_id
        val userJson: String = jedis.get(userKey)
        if (userJson != null && userJson.length > 0) {
          val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)
          saleDetailListBuffer += saleDetail
        }
      }
      jedis.close()
      saleDetailListBuffer.toIterator
    }
    saleDetailFinalDstream.print()




    //双流join
    //val orderJoinedDstream: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoWithIdDstream.join(orderDetailWithOidDstream)
     //orderJoinedDstream.print()


    //写入到ES
    //1  建索引

    //2  写入
    saleDetailFinalDstream.foreachRDD{rdd=>
      rdd.foreachPartition { saleDetailItr =>
        val saleDetailWithIdList: List[(String, SaleDetail)] = saleDetailItr.map(saleDetail=>(saleDetail.order_detail_id,saleDetail)).toList
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_SALE,saleDetailWithIdList)

      }

    }


    ssc.start()

    ssc.awaitTermination()

  }

}
