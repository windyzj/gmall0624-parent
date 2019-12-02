package com.atguigu.gmall0624.realtime.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0624.bean.UserInfo
import com.atguigu.gmall0624.common.constant.GmallConstant
import com.atguigu.gmall0624.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("user_app")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val userInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_USER,ssc)

    val userInfoDstream: DStream[UserInfo] = userInputDstream.map(_.value()).map(userJson=>JSON.parseObject(userJson,classOf[UserInfo]))

    userInfoDstream.print()

    //写入成为维表
    userInfoDstream.foreachRDD{rdd=>

      rdd.foreachPartition { userInfoItr =>
          val jedis: Jedis = RedisUtil.getJedisClient
         // userkey     type   string    key   user_info:[user_id]  value user_info_json
        for (userInfo <- userInfoItr ) {
          val userkey="user_info:"+userInfo.id
          val userJson: String = JSON.toJSONString(userInfo,new SerializeConfig(true))
          jedis.set(userkey,userJson)
        }
        jedis.close()

      }

    }


    ssc.start()
    ssc.awaitTermination()

  }

}
