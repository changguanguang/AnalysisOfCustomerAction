package request2

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject

object PageConvertStat {

  def getUserVisitAction(sparkSession: SparkSession, taskParam: json.JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)


    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" +
      endDate + "'"

    import sparkSession.implicits._
    // 先转化为DataSet,然后再转化为rdd
    // 就会得到 RDD[UserVisitAction] 而不是RDD[Row]
    // 从而 可以对RDD中的对象调用属性
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))


  }

  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     startPageCount: Long,
                     pageSplitCountMap: collection.Map[String, Long])= {

    val pageSplitRatioMap = new mutable.HashMap[String,Double]
    var lastPageCount = startPageCount.toDouble

    for(pageSplit <- targetPageSplit){

      val currentPageSplitCount = pageSplitCountMap.get(pageSplit).getOrElse(0L).toDouble



      val ratio = currentPageSplitCount / lastPageCount

      pageSplitRatioMap.put(pageSplit,ratio)

      lastPageCount = currentPageSplitCount
    }

    val convertStr = pageSplitRatioMap.map{
      case (pageSplit,ratio) =>{
        pageSplit + "=" + ratio
      }
    }.mkString("|")

    // 页面转化率只有一条信息,所以是一个RDD[PageSplitConvertRate]
    val pageSplit = PageSplitConvertRate(taskUUID,convertStr)

    val pageSplitRatioRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))

    import sparkSession.implicits._

    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate_0308")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }

  def main(args: Array[String]): Unit = {


    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    // 将字符串变为json对象
    val taskParam = json.JSONObject.fromObject(jsonStr)

    val taskUUID = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取用户行为数据
    val sessionId2ActionRDD = getUserVisitAction(sparkSession, taskParam)

    //
    val pageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)

    val pageFlowArray = pageFlowStr.split(",")

    // targetPageSplit : {1_2,2_3,3_4,4_5,...}
    val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => {
        page1 + "_" + page2
      }
    }

    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()


    // pageSplitNumRDD : RDD[pageId,count]
    val pageSplitNumRDD = sessionId2GroupRDD.flatMap {

      case (sessionId, iterableAction) => {

        // 在这里都是对集合itrable,list,map等进行操作


        // sortList : List[UserVisitAction]
        val sortList = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })

        val pageList = sortList.map {
          // case action => action.page_id
          _.page_id
        }

        // pageSplit : List[String]
        val pageSplit = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }


        val pageSplitFilter = pageSplit.filter {
          case tmpPageSplit => targetPageSplit.contains(tmpPageSplit)
        }
//        println(pageSplitFilter.mkString(","))


        // 作为结果返回
        pageSplitFilter.map {
          (_, 1L)
        }
      }
    }

    // Map [(pageSplit,count)]
    //
    val pageSplitCountMap = pageSplitNumRDD.countByKey()

    val startPage = pageFlowArray(0).toLong

    val startPageCount = sessionId2ActionRDD.filter({
      case (sessionId,action) => {
        action.page_id == startPage
      }
    }).count()
    targetPageSplit.foreach(println(_))
    println(" startPageCount = " + startPageCount)
    println(pageSplitCountMap.mkString("|"))

    getPageConvert(sparkSession,taskUUID,targetPageSplit,startPageCount,pageSplitCountMap)


  }

}
