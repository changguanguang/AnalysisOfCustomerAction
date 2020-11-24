package request4

import java.sql.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer


object AdverStat {


  def generateBlackList(adRealTimeFilterDStream: DStream[String]): Unit = {
    println("come into generateBlackList")
    val key2NumDStream = adRealTimeFilterDStream.map {
      log => {
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adId = logSplit(4).toLong

        val key = dateKey + "_" + userId + "_" + adId

        (key, 1L)
      }
    }

    // reduceByKey ： 对于每个RDD使用reduceByKey，得到每个RDD上的点击次数聚合
    val key2CountDStream = key2NumDStream.reduceByKey(_ + _)


    //    println("is ready to write into mysql ad_click_count_stat")
    key2CountDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items => {
            val clickCountArray = new ArrayBuffer[AdUserClickCount]()

            for ((key, count) <- items) {
              val keySplit = key.split("_")
              val date = keySplit(0)
              val userId = keySplit(1).toLong
              val adid = keySplit(2).toLong

              clickCountArray += AdUserClickCount(date, userId, adid, count)
            }
            // 这里使用到数据库连接，所以为了减少资源耗费，使用partition
            // 因为RDD里面的数据已经默认按照Hash分区分区好了。所以同一个人的数据
            // 一定在一个partition分区里面，所以可以直接使用partition进行更新数据库的数据
            AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
          }
        }

    }


    // 从数据库中获取已经写入到数据库中的每个用户的点击次数
    val key2BlackListDStream = key2CountDStream.filter {
      case (key, count) => {

        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong

        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)
        if (clickCount > 30) {
          true
        } else {
          false
        }
      }
    }
    // 直接从RDD中过滤得到黑名单
    //    val key2BlackListDStream2 = key2CountDStream.filter{
    //      case (key,count) =>{
    //        if(count >100){
    //          true
    //        }else{
    //          false
    //        }
    //      }
    //    }


    // 在初始的过滤中，key是 dateKey + "_" + userId + "_" + adId
    // 当将key截取一部分为 userid的时候，这时候之前过滤的数据就很有可能出现
    // 重复数据，因此需要进行distinct
    val userIdDStream = key2BlackListDStream.map {
      case (key, count) => {
        key.split("_")(1).toLong
      }
    }.transform(rdd => rdd.distinct())

    userIdDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items => {
            val userIdArray = new ArrayBuffer[AdBlacklist]()
            for (userId <- items) {
              userIdArray += AdBlacklist(userId)

              // 插入DStream的数据到mysql中，那么就需要使用partition批量插入
              AdBlacklistDAO.insertBatch(userIdArray.toArray)
            }
          }
        }
    }
  }


  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {
    val key2ProvinceCityDStream = adRealTimeFilterDStream.map {
      case log => {
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // dateKey : yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val province = logSplit(1)
        val city = logSplit(2)
        val adid = logSplit(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
      }
    }
    // 求各省各城市广告一天中的点击量，会跨越 RDD 进行操作，因此使用updateStateByKey
    // key2StateDStream : DStream[RDD[key,value]]
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) => {
        var newValue = 0L
        newValue = state.getOrElse(0L)
        for (value <- values) {
          newValue += value
        }
        Some(newValue)
      }
    }

    // 这里的key2StateDStream已经成型，可以直接批量写入mysql
    // 只需要把字段进行截取封装

    key2StateDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val adStatArray = new ArrayBuffer[AdStat]()
            for ((key, count) <- items) {
              val keySplit = key.split("_")
              val date = keySplit(0)
              val province = keySplit(1)
              val city = keySplit(2)
              val adid = keySplit(3).toLong

              adStatArray += AdStat(date, province, city, adid, count)
            }
            AdStatDAO.updateBatch(adStatArray.toArray)
        }
    }
    key2StateDStream
  }

  def provinceTop3Adver(sparkSession: SparkSession,
                        key2ProvinceCityCountDStream: DStream[(String, Long)]): Unit = {
    // key2ProvinceCityCountDStream:DStream[RDD[(key,count)]]
    // key :

    // 对每个RDD中的所有数据进行处理，维度削减
    val key2ProvinceCountDStream = key2ProvinceCityCountDStream.map {
      case (key, count) => {
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)

        val newKey = date + "_" + province + "_" + adid
        (newKey, count)
      }
    }

    // 经过键的截取，数据会出现重复，需要再一次聚合
    val key2ProvinceAggrCountDStream = key2ProvinceCountDStream.reduceByKey(_ + _)

    // 这里使用上次的有状态转化后的数据,所以直接对每一个RDD进行操作就好
    // 每一个DStream中的数据就是没有时间维度的数据，只是一些Hash分区后的全量数据
    val top3DStream = key2ProvinceAggrCountDStream.transform {
      rdd => {
        val basicDateRDD = rdd.map {
          case (key, count) => {
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong

            (date, province, adid, count)
          }
        }
        import sparkSession.implicits._
        basicDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")

        val sql = "select date, province, adid, count from(" +
          "select date, province, adid, count, " +
          "row_number() over(partition by date,province order by count desc) rank from tmp_basic_info) t " +
          "where rank <= 3"
        sparkSession.sql(sql).rdd
      }
    }


    // top3DStream 就是最终成型的结果RDD
    //    现在只需要对每个RDD中的数据进行批量更新

    top3DStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for (item <- items) {
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)

            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
      }
    }

  }

  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]): Unit = {
    // adRealTimeFilterRDD:  RDD[timestamp + " " + province + " " + city + " " + userid + " " + adid]

    val key2TimeMinuteDStream = adRealTimeFilterDStream.map {
      case log => {
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adid = logSplit(4).toLong

        val key = timeMinute + "_" + adid
        (key, 1L)
      }
    }

    val key2WindowDStream = key2TimeMinuteDStream.reduceByKeyAndWindow((a: Long, b: Long) => (a + b), Minutes(60), Minutes(1))

    //    key2TimeMinuteDStream,此时已经成型，直接批量写入
    key2WindowDStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          items => {
            val trendArray = new ArrayBuffer[AdClickTrend]()
            for ((key, count) <- items) {
              // key :timeMinute + "_" +adid
              val keySplit = key.split("_")
              // yyyyMMddHHmm
              val timeMinute = keySplit(0)
              val date = timeMinute.substring(0, 8)
              val hour = timeMinute.substring(8, 10)
              val minute = timeMinute.substring(10)
              val adid = keySplit(1).toLong

              trendArray += AdClickTrend(date, hour, minute, adid, count)

            }
            AdClickTrendDAO.updateBatch(trendArray.toArray)
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    //    val sparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    //
    //    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    //
    //    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    //    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)
    //
    //    val kafkaParam = Map(
    //      "bootstrap.servers" -> kafka_brokers,
    //      "key.deserializer" -> classOf[StringDeserializer],
    //      "value.deserializer" -> classOf[StringDeserializer],
    //      "group.id" -> "group1",
    //      "auto.offset.reset" -> "latest",
    //      "enable.auto.commit" -> (false: java.lang.Boolean),
    //      "zookeeper.connect" -> "hadoop101:2181,hadoop102:2181,hadoop103:2181"
    //    )
    //
    //    //    import org.apache.spark.streaming
    //
    //    // adRealTimeDStream : DStream[RDD RDD RDD...] RDD[message] message:key value
    //    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent,
    //      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam))


    // @TODO 创建sparkSession -> streamingContext
    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // val streamingContext = StreamingContext.getActiveOrCreate(checkpointDir, func)
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    // @TODO 设置kafka的必要参数
    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
      // earlist: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "zookeeper.connect" -> "hadoop101:2181,hadoop102:2181,hadoop103:2181"
    )

    // adRealTimeDStream: DStream[RDD RDD RDD ...]  RDD[message]  message: key value
    // @TODO 创建kafka-sparkStreaming输入流
    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    println("===================================================")
    // 将kafka中接收到的数据取出key值，只要value值
    val adRealTimeValueDStream = adRealTimeDStream.map {
      item =>
        item.value()
    }


    // @TODO 过滤黑名单中的数据
    //    timestamp + " " + province + " " + city + " " + userid + " " + adid
    // 进行实时过滤，将过来的数据随时与黑名单的userId进行比对
    val adRealTimeFilterDStream = adRealTimeValueDStream.transform {
      logRDD => {

        val blackListArray = AdBlacklistDAO.findAll()
        val blackUserIdList = blackListArray.map {
          item => item.userid
        }
        logRDD.filter {
          item => {
            val userId = item.split(" ")(3)
            if (blackUserIdList.contains(userId)) {
              false
            } else {
              true
            }
          }
        }
        logRDD
      }
    }
    adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println(_)))
    //    val adRealFilter2 = adRealTimeValueDStream.filter {
    //      item => {
    //        val blackListArray = AdBlacklistDAO.findAll()
    //        val blackUserIdList = blackListArray.map {
    //          item => item.userid
    //        }
    //        val userId = item.split(" ")(3)
    //        if (blackUserIdList.contains(userId)) {
    //          false
    //        } else {
    //          true
    //        }
    //      }
    //    }


    //@TODO 设置 updateState 所需要的检查点 checkPoint
    streamingContext.checkpoint("./spark-streaming")
    adRealTimeFilterDStream.checkpoint(Duration(10000))


    // @TODO 实现各个需求
    // 需求一： 实时维护黑名单
    generateBlackList(adRealTimeFilterDStream)


    // 需求二： 各省各城市一天中的广告点击量（类及统计）
    // key2ProvinceCityCountDStream : DStream[RDD[(key,count)],RDD[(key,count)]]
    //   key: province_city_adid
    val key2ProvinceCityCountDStream = provinceCityClickStat(adRealTimeFilterDStream)

    // 需求三： 统计各省top3热门广告
    provinceTop3Adver(sparkSession, key2ProvinceCityCountDStream)


    // 需求四： 最近一个小时广告的点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
