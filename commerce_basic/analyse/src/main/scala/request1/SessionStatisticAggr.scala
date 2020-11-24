package request1

import java.util
import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SessionStatisticAggr {

  //
  // 这里的sql中字符串拼接一定要加 引号'',不然识别不了
  def getOriActionRDD(sparkSession: SparkSession, taskParam: json.JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >= '" +
      startDate +
      "' and date <= '" +
      endDate +
      "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

  def getSessionFullInfo(sparkSession: SparkSession, sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = sessionId2GroupActionRDD.map {
      case (sessionId, iterableAction) => {
        var userId = -1L
        var startTime: util.Date = null
        var endTime: util.Date = null

        var stepLength = 0

        var searchKeywords = new StringBuffer("")
        var clickCategories = new StringBuffer("")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }
          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }
          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1
        }
        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)


        (userId, aggrInfo)

      }
    }

    val sql = "select * from user_info"

    import sparkSession.implicits._

        val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map {
          item => (item.user_id, item)
        }

        val userId222 = sparkSession.sql(sql).as[]
//    val userId2InfoRDD = sparkSession.sql(sql).rdd.map {
//      item => (item.user_id, item)
//    }

    val sessionId2FullInfoRDD = userId2InfoRDD.join(userId2AggrInfoRDD).map {
      case (userId, (userInfo, aggrInfo)) => {
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
      }
    }
    sessionId2FullInfoRDD
  }


  def calculateVisitLength(visitLength: Long, sessionStatisticAccumulator: SessionAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: SessionAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getSessionFilteredRDD(taskParam: json.JSONObject,
                            sessionId2FullInfoRDD: RDD[(String, String)],
                            sessionAccumulator: SessionAccumulator) = {
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    //    println("sdcscsdkmsk")
    //    sessionId2FullInfoRDD.foreach(println(_))
    val sessionFilterRDD = sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) => {
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          sessionAccumulator.add(Constants.SESSION_COUNT)

          println(fullInfo)
          println(StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH))
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)
        }
        success
      }
    }
    sessionFilterRDD
  }


  def getSessionRatio(sparkSession: SparkSession,
                      taskUUID: String,
                      value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(stat))
    sessionRatioRDD.foreach(println(_))

    // 将ratioRDD转化为DF存储到mysql 数据库表里
    import sparkSession.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio_0416")
      .mode("append")
      .save()

  }

  def generateRandomIndexList(extractPerDay: Int,
                              daySessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]) = {

    for ((hour, count) <- hourCountMap) {
      // 获取一个小时要抽取多少数据
      var hourExrCount = ((count / daySessionCount.toDouble) * extractPerDay).toInt

      // 避免一个小时要抽取的数量超过这个小时总数
      if (hourExrCount > count) {
        hourExrCount = count.toInt
      }

      var random = new Random()
      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (i <- 0 until hourExrCount) {
            //一共获得 hourExrCount 个随机数字，这些数字的范围位于【0，count】
            // count是这个小时的数据数量
            var index = random.nextInt(count.toInt)
            // 避免得到重复的随机数
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            // 将抽取到的随机数加入到 hourListMap 中
            hourListMap(hour).append(index)
          }
        case list =>
          for (i <- 0 until hourExrCount) {
            //一共获得 hourExrCount 个随机数字，这些数字的范围位于【0，count】
            // count是这个小时的数据数量
            var index = random.nextInt(count.toInt)
            // 避免得到重复的随机数
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            // 将抽取到的随机数加入到 hourListMap 中
            hourListMap(hour).append(index)
          }
      }
    }
  }


  def sessionRandomExtract(sparkSession: SparkSession,
                           taskUUID: String,
                           sessionId2FilterRDD: RDD[(String, String)]) = {
    // dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
    val dateHour2FullInfoRDD = sessionId2FilterRDD.map {
      case (sid, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        // dateHour: yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
    }

    // hourCountMap: Map[(dateHour, count)]
    val hourCountMap = dateHour2FullInfoRDD.countByKey()

    // dateHourCountMap: Map[(date, Map[(hour, count)])]
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }
    }

    // 解决问题一： 一共有多少天： dateHourCountMap.size
    //              一天抽取多少条：100 / dateHourCountMap.size
    val extractPerDay = 100 / dateHourCountMap.size

    // 解决问题二： 一天有多少session：dateHourCountMap(date).values.sum
    // 解决问题三： 一个小时有多少session：dateHourCountMap(date)(hour)

    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    // dateHourCountMap: Map[(date, Map[(hour, count)])]
    for ((date, hourCountMap) <- dateHourCountMap) {
      val dateSessionCount = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
      }
    }

    // 到目前为止，我们获得了每个小时要抽取的session的index

    // 广播大变量，提升任务性能 在driver中创建好广播变量，进行广播，给每个节点发送一份
    val dateHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)

    // dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
    // dateHour2GroupRDD: RDD[(dateHour, iterableFullInfo)]
    val dateHour2GroupRDD = dateHour2FullInfoRDD.groupByKey()

    // extractSessionRDD: RDD[SessionRandomExtract]
    val extractSessionRDD = dateHour2GroupRDD.flatMap {
      case (dateHour, iterableFullInfo) =>
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)

        // 从广播变量中获取特定的list
        val extractList = dateHourExtractIndexListMapBd.value.get(date).get(hour)

        val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

        var index = 0

        // 对于迭代器中的数据进行处理
        for (fullInfo <- iterableFullInfo) {
          if (extractList.contains(index)) {
            val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

            val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)

            extractSessionArrayBuffer += extractSession
          }
          index += 1
        }

        extractSessionArrayBuffer
    }

    import sparkSession.implicits._
    extractSessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_extract_0308")
      .mode(SaveMode.Append)
      .save()
    extractSessionRDD
  }


  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    // 先进行过滤，把点击行为对应的action保留下来
    val clickFilterRDD = sessionId2FilterActionRDD.filter {
      item => item._2.click_category_id != null
    }

    val clickNumRDD = clickFilterRDD.map({
      case (sessionId, action) => {
        (action.click_category_id, 1L)
      }
    })

    // 这里的reduceBykey 只能对于RDD[a,b]使用,a作为key,b作为value
    val clickNumAggrRDD = clickNumRDD.reduceByKey(_ + _)

    clickNumAggrRDD

  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.order_category_ids != null)

    // flatmap: 先将action --map---> Array --flatten--> (item,1L)
    val orderNumRDD = orderFilterRDD.flatMap {
      // action.order_category_ids.split(","): Array[String]
      // action.order_category_ids.split(",").map(item => (item.toLong, 1L)
      // 先将我们的字符串拆分成字符串数组，然后使用map转化数组中的每个元素，
      // 原来我们的每一个元素都是一个string，现在转化为（long, 1L）
      case (sessionId, action) => action.order_category_ids.split(",").
        map(item => (item.toLong, 1L))
    }
    orderNumRDD.reduceByKey(_ + _)
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids != null)

    val payNumRDD = payFilterRDD.flatMap {
      case (sid, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    payNumRDD.reduceByKey(_ + _)
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) => {
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, aggrCount)
      }
    }

    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
          Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)
    }
    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)
    }

    cid2PayInfoRDD
  }

  //
  //  def top10PopularCategories(sparkSession: SparkSession,
  //                             taskUUID: String,
  //                             sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
  //    // 第一步：获取所有发生过点击、下单、付款的品类
  //    var cid2CidRDD = sessionId2FilterActionRDD.flatMap{
  //      case (sid, action)=>
  //        val categoryBuffer = new ArrayBuffer[(Long, Long)]()
  //
  //        // 点击行为
  //        if(action.click_category_id != -1){
  //          categoryBuffer += ((action.click_category_id, action.click_category_id))
  //        }else if(action.order_category_ids != null){
  //          for(orderCid <- action.order_category_ids.split(","))
  //            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
  //        }else if(action.pay_category_ids != null){
  //          for(payCid <- action.pay_category_ids.split(","))
  //            categoryBuffer += ((payCid.toLong, payCid.toLong))
  //        }
  //        categoryBuffer
  //    }
  //
  //    cid2CidRDD = cid2CidRDD.distinct()
  //
  //    // 第二步：统计品类的点击次数、下单次数、付款次数
  //    val cid2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)
  //
  //    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActionRDD)
  //
  //    val cid2PayCountRDD = getPayCount(sessionId2FilterActionRDD)
  //
  //    // cid2FullCountRDD: RDD[(cid, countInfo)]
  //    // (62,categoryid=62|clickCount=77|orderCount=65|payCount=67)
  //    val cid2FullCountRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)
  //
  //    // 实现自定义二次排序key
  //    val sortKey2FullCountRDD = cid2FullCountRDD.map{
  //      case (cid, countInfo) =>
  //        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
  //        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
  //        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
  //
  //        val sortKey = SortKey(clickCount, orderCount, payCount)
  //
  //        (sortKey, countInfo)
  //    }
  //
  //    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)
  //
  //    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map{
  //      case (sortKey, countInfo) =>
  //        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
  //        val clickCount = sortKey.clickCount
  //        val orderCount = sortKey.orderCount
  //        val payCount = sortKey.payCount
  //
  //        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
  //    }
  //
  //    import sparkSession.implicits._
  //    top10CategoryRDD.toDF().write
  //      .format("jdbc")
  //      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
  //      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
  //      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
  //      .option("dbtable", "top10_category_0308")
  //      .mode(SaveMode.Append)
  //      .save
  //
  //    top10CategoryArray
  //  }
  //

  def top10PopularCategories(sparkSession: SparkSession,
                             taskUUID: String,
                             sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    // 获取所有发生过点击，下单，付款的品类
    var cid2CidRDD = sessionId2FilterActionRDD.flatMap {
      case (sid, action) => {
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        // 点击行为
        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
        } else if (action.pay_category_ids != null) {
          for (payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }

        categoryBuffer
      }
    }

    cid2CidRDD = cid2CidRDD.distinct()

    // 统计品类的点击次数，下单次数，付款次数
    // cid2ClickCountRDD : RDD[ category : Long,count : Long]
    val cid2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)

    //cid2OrderCountRDD : RDD[ orderId:Long,count :Long ]
    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActionRDD)

    // cid2PayCountRDD : RDD[ pay_id:Long,count :Long ]
    val cid2PayCountRDD = getPayCount(sessionId2FilterActionRDD)

    // cid2FullCountRDD: RDD[(cid, countInfo)]
    // (62,categoryid=62|clickCount=77|orderCount=65|payCount=67)
    val cid2FullCountRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)

    // 实现自定义的二次排序
    // cid2FullCountRDD: RDD[(cid, countInfo)]
    // (62,categoryid=62|clickCount=77|orderCount=65|payCount=67)
    val sortKey2FullCountRDD = cid2FullCountRDD.map {
      case (cid, countInfo) => {
        print("countinfo = " + countInfo)
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)

        (sortKey, countInfo)
      }
    }

    // top10CategoryArray : Array[(SortKey,String)]
    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)


    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, countInfo) => {
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount

        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
      }
    }

    import sparkSession.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category_0308")
      .mode(SaveMode.Append)
      .save

    //top10CategoryArray : Array [ (sortKey,CountInfo) ]
    top10CategoryArray
  }


  def top10ActiveSession(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]) = {
    // cidArray[category_id]
    val cidArray = top10CategoryArray.map {
      case (sortKey, countInfo) => {
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
      }
    }

    // 所有符合过滤条件的，并且点击过top10热门品类的action
    // 两种做法：
    // 1. join
    // 2. filter
    // 这里采用的第二种做法
    val sessionId2ActionRDD = sessionId2FilterActionRDD.filter {
      case (session_id, action) =>
        cidArray.contains(action.click_category_id)
    }

    // 按照sessionId进行聚合
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()


    //cid2SessionCountRDD：RDD[ category_id,"sessionId=count" ]
    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) => {
        val categoryCountMap = new mutable.HashMap[Long, Long]()

        for (action <- iterableAction) {
          val cid = action.click_category_id
          if (!categoryCountMap.contains(cid)) {
            categoryCountMap += (cid -> 0)
          }
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }

        // 记录了一个session对于它所有点击过的品类的点击次数
        for ((cid, count) <- categoryCountMap)
        //将每一个元素映射为一个数组，这个数组然后会调用flatten扁平化
          yield (cid, sessionId + "=" + count)
      }
    }

    // cid2SessionCountRDD: RDD [ Long,iterable[String] ]
    val cid2GroupRDD = cid2SessionCountRDD.groupByKey()


    val top10SessionRdd = cid2GroupRDD.flatMap {
      case (cid, iterableSessionCount) => {

        val sortList = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)


        // top10Session : List[Top10Session]
        val top10Session = sortList.map {
          case item => {
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong

            Top10Session(taskUUID, cid, sessionId, count)
          }
        }

        top10Session
        //        new ArrayBuffer()
      }
    }
    import sparkSession.implicits._
    top10SessionRdd.toDF.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_0308")
      .mode(SaveMode.Append)
      .save()


  }

  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    // 将jsonStr转化为jsonObject
    val taskParam = json.JSONObject.fromObject(jsonStr)

    val taskUUID = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setAppName("session2").setMaster("local[*]")

    // 创建sparkSession 包含SparkContext
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取原始的动作数据表
    // actionRDD ：[ UserVisitAction ]
    val actionRDD = getOriActionRDD(sparkSession, taskParam)

    //    actionRDD.foreach(println)

    // sessionRDD : [ session_id,UserVisitAction ]
    val sessionId2ActionRDD = actionRDD.map(
      item => (item.session_id, item)
    )
    // sessionId2GroupActionRDD : [ sessionId , Iterable[UserVisitAction] ]
    val sessionId2GroupActionRDD = sessionId2ActionRDD.groupByKey()

    // 什么时候用cache？ 在后边的几个RDD都使用这个RDD时候,就可以设置cache,减少计算,加快速度
    sessionId2ActionRDD.cache()

    // sessionId2FullInfoRDD : RDD[  (sessionId,fullInfo) ]
    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, sessionId2GroupActionRDD)
    //    sessionId2FullInfoRDD.foreach(println(_))

    // 创建累加器,注册累加器
    val sessionAccumulator = new request1.SessionAccumulator

    sparkSession.sparkContext.register(sessionAccumulator, "sessionAccumulator")

    // sessionIdFilterRDD : RDD[(sessionId,fullInfo)] 所有符合过滤条件的数据组成的RDD
    // getSessionFilteredRDD 根据限制条件进行过滤,刚好遍历了一遍,顺便并完成累加器的更新,
    println("come into filter function")
    val sessionIdFilterRDD = getSessionFilteredRDD(taskParam, sessionId2FullInfoRDD, sessionAccumulator)

    // 打印过滤后的数据 动作算子,将前面的逻辑计数进行执行
    sessionIdFilterRDD.foreach(println)
    // 将sessionRatio写入到mysql中
    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)



    // 需求二
    // session随机抽取

    val sessionRandomExtractRDD = sessionRandomExtract(sparkSession, taskUUID, sessionIdFilterRDD)
    val sessionId2FilterActionRDD = sessionId2ActionRDD.join(sessionIdFilterRDD).map {
      case (sessionId, (action, fullInfo)) => {
        (sessionId, action)
      }
    }
    // extractSessionsRDD : RDD[ SessionRandomExtract ]
    val extractSessionsRDD = sessionRandomExtractRDD.map {
      case (sessionRandomExtract) => {
        (sessionRandomExtract.sessionid, sessionRandomExtract.sessionid)
      }
    }

    //extractSessionDetailRDD : RDD[ SessionDetail ]
    val extractSessionDetailRDD = extractSessionsRDD.join(sessionId2FilterActionRDD).map {
      case (sessionId, (sessionId2, action)) => {
        SessionDetail(taskUUID, action.user_id, action.session_id,
          action.page_id, action.action_time, action.search_keyword,
          action.click_category_id, action.click_product_id,
          action.order_category_ids, action.order_product_ids,
          action.pay_category_ids, action.pay_product_ids)
      }
    }
    // 将SessionDetailsRDD 以DF的格式保存到mysql中
    import sparkSession.implicits._
    extractSessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_detail")
      .mode(SaveMode.Append)
      .save()






    //    val cid2CidRDD = sparkSession.sparkContext.makeRDD()


    // 需求三
    // Top10热门品类
    val top10CategoryArray = top10PopularCategories(sparkSession, taskUUID, sessionId2FilterActionRDD)

    // cid2CidRDD : RDD [ Long,Long ]
    val cid2CidRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, countInfo) => {
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong

        (cid, cid)
      }
    }

    // 需求四：
    // top10category的top10Session

    top10ActiveSession(sparkSession, taskUUID, sessionId2FilterActionRDD, top10CategoryArray)


  }


}
