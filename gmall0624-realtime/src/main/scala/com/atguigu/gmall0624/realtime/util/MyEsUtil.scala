package com.atguigu.gmall0624.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

object MyEsUtil {

  //创建客户端
  private val ES_HOST = "http://hadoop1"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if ( client!=null) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }

//  batch  bulk
  def main(args: Array[String]): Unit = {
    //单条保存
/*    val jest: JestClient = getClient
    //any  :  Map
    val index: Index = new Index.Builder(Stu(1,"zhang3",624)).index("stu0624").`type`("stu").id("1").build()
    jest.execute(index)
    close(jest)*/
    //多条
    val stuList = List(("2",Stu(2,"li4",624)),("3",Stu(3,"wang5",624)),("4",Stu(4,"zhao6",624)))
    insertBulk(stuList)
  }


  def insertBulk(list:List[(String,Any)]): Unit ={
    val jest: JestClient = getClient

    val bulkBuilder = new Bulk.Builder()
    for ( (id,doc) <- list ) {
      val index: Index  = new Index.Builder(doc).index("stu0624").`type`("stu").id(id).build()
      bulkBuilder.addAction(index)
    }
    jest.execute(bulkBuilder.build())
    close(jest)
  }

  case class Stu(id:Int,name:String,classId:Int)
}
