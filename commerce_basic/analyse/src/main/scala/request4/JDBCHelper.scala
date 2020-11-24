package request4

import java.sql.ResultSet

import commons.pool.{CreateMySqlPool, QueryCallback}

import scala.collection.mutable.ArrayBuffer


/**
  * 用户黑名单DAO类
//  */
//object AdBlacklistDAO {
//
//  def insertBatch(adBlacklists : Array[AdBlacklist]): Unit ={
//
//    val sql = "insert into ad_blacklist values(?)"
//
//    val paramsList = new ArrayBuffer[Array[Any]]()
//
//    // 向paramsList添加userId
//    for(adBlackList <- adBlacklists){
//      val params:Array[Any] =Array(adBlackList.userid)
//      paramsList += params
//    }
//
//    // 获取对象池的单例对象 （括号也不是可以随便省略，）
//    val mysqlPool = CreateMySqlPool()
//
//    // 从对象池中获取对象
//    val client = mysqlPool.borrowObject()
//
//    // 执行批量插入操作
//    client.executeBatch(sql,paramsList.toArray)
//
//    // 使用完成后将对象返回给对象池
//    mysqlPool.returnObject(client)
//
//  }
//
//  /**
//    * 查询所有广告黑名单用户
//    */
//  def findAll()={
//    val sql = "select * from ad_blacklist"
//
//    val adBlacklists = new ArrayBuffer[AdBlacklist]()
//
//    val mysqlPool = CreateMySqlPool()
//    val client = mysqlPool.borrowObject()
//
//    client.executeQuery(sql,null,new QueryCallback {
//      override def process(rs: ResultSet): Unit = {
//        while(rs.next()){
//          val userid = rs.getInt(1).toLong
//          adBlacklists += AdBlacklist(userid)
//        }
//      }
//    })
//
//    // 将对象返回给对象池
//    mysqlPool.returnObject(client)
//    adBlacklists.toArray
//
//  }
//
//}
//
//object AdProvinceTop3DAO {
//
//  def updateBatch(adProvinceTop3s: Array[AdProvinceTop3]) {
//    // 获取对象池单例对象
//    val mySqlPool = CreateMySqlPool()
//    // 从对象池中提取对象
//    val client = mySqlPool.borrowObject()
//
//    // dateProvinces可以实现一次去重
//    // AdProvinceTop3：date province adid clickCount，由于每条数据由date province adid组成
//    // 当只取date province时，一定会有重复的情况
//    val dateProvinces = ArrayBuffer[String]()
//
//    for (adProvinceTop3 <- adProvinceTop3s) {
//      // 组合新key
//      val key = adProvinceTop3.date + "_" + adProvinceTop3.province
//
//      // dateProvinces中不包含当前key才添加
//      // 借此去重
//      if (!dateProvinces.contains(key)) {
//        dateProvinces += key
//      }
//    }
//
//    // 根据去重后的date和province，进行批量删除操作
//    // 先将原来的数据全部删除
//    val deleteSQL = "DELETE FROM ad_province_top3 WHERE date=? AND province=?"
//
//    val deleteParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
//
//    for (dateProvince <- dateProvinces) {
//
//      val dateProvinceSplited = dateProvince.split("_")
//      val date = dateProvinceSplited(0)
//      val province = dateProvinceSplited(1)
//
//      val params = Array[Any](date, province)
//      deleteParamsList += params
//    }
//
//    client.executeBatch(deleteSQL, deleteParamsList.toArray)
//
//    // 批量插入传入进来的所有数据
//    val insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)"
//
//    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
//
//    // 将传入的数据转化为参数列表
//    for (adProvinceTop3 <- adProvinceTop3s) {
//      insertParamsList += Array[Any](adProvinceTop3.date, adProvinceTop3.province, adProvinceTop3.adid, adProvinceTop3.clickCount)
//    }
//
//    client.executeBatch(insertSQL, insertParamsList.toArray)
//
//    // 使用完成后将对象返回给对象池
//    mySqlPool.returnObject(client)
//  }
//
//}


