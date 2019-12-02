package com.atguigu.gmall0624.realtime.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig}
import com.atguigu.gmall0624.bean.SaleDetail

object TestApp {

  def main(args: Array[String]): Unit = {
      val detail = new SaleDetail("123","3434")
      val config = new SerializeConfig(true)
      val str: String = JSON.toJSONString(detail, new SerializeConfig(true )  )

    println(str)
  }

}
