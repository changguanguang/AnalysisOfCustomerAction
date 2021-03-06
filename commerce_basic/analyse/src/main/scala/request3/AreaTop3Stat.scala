package request3

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaTop3Stat {

  def getCityAndProductInfo(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    // 只获取发生过点击的action的数据
    // 获取到的一条action数据就代表一个点击行为
    val sql = "select city_id, click_product_id from user_visit_action where date>='" + startDate +
      "' and date<='" + endDate + "' and click_product_id != -1"

    import sparkSession.implicits._

    sparkSession.sql(sql).as[CityClickProduct].rdd.map {
      case cityPid => (cityPid.city_id, cityPid.click_product_id)
    }

  }

  def getCityAreaInfo(sparkSession: SparkSession) = {
    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    sparkSession.sparkContext.makeRDD(cityAreaInfoArray).map {
      case (cityId, cityName, area) => {
        (cityId, CityAreaInfo(cityId, cityName, area))
      }
    }
  }


  // 生成一个临时视图
  def getAreaPidBasicInfoTable(sparkSession: SparkSession, cityId2PidRDD: RDD[(Long, Long)], cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]) = {
    val areaPidInfoRDD = cityId2PidRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, areaInfo)) => {
        (cityId, areaInfo.city_name, areaInfo.area, pid)
      }
    }

    import sparkSession.implicits._
    areaPidInfoRDD.toDF("city_id", "city_name", "area", "pid").createOrReplaceTempView("tmp_area_basic_info")

  }

  def getAreaProductClickCountTable(sparkSession: SparkSession): Unit = {
    val sql = "select area,pid,count(*) click_count ," +
      " group_concat_distinct(concat_long_string(city_id,city_name,':')) city_Infos" +
      " from tmp_area_basic_info group by area,pid"
    //
    //    val sql = "select area, pid, count(*) click_count," +
    //      " group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos" +
    //      " from tmp_area_basic_info group by area, pid"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_click_count")
  }

  //
  def getTop3Product(sparkSession: SparkSession, taskUUID: String): Unit = {

    val sql = "select area, " +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A_Level' " +
      "WHEN area='华中' OR area='华南' THEN 'B_Level' " +
      "WHEN area='西南' OR area='西北' THEN 'C_Level' " +
      "ELSE 'D_Level' " +
      "END area_level, " +
      "city_infos, pid, product_name, product_status, click_count from (" +
      "select area, city_infos, pid, product_name, product_status, click_count, " +
      "row_number() over(PARTITION BY area ORDER BY click_count DESC) rank from " +
      "tmp_area_count_product_info) t where rank<=3"

    // 这里将DF的每一行map为一个 class AreaTop3Product
    // 由于DF也有字段名称，所以也可以实现
    import sparkSession.implicits._
    val top3RDD = sparkSession.sql(sql).as[AreaTop3Product].rdd

    val top3ProductRDD = sparkSession.sql(sql).rdd.map {
      case row => {
        AreaTop3Product(taskUUID,
          row.getAs[String]("area"), row.getAs[String]("area_level"),
          row.getAs[Long]("pid"), row.getAs[String]("city_infos"),
          row.getAs[Long]("click_count"), row.getAs[String]("product_name"),
          row.getAs[String]("product_status")
        )
      }
    }

    import sparkSession.implicits._

    top3ProductRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product_0308")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }

  def getAreaProductClickCountInfo(sparkSession: SparkSession): Unit = {
    val sql = "select " +
      "tacc.area,tacc.city_infos,tacc.pid,pi.product_name," +
      "if(get_json_field(pi.extend_info, 'product_status')='0','Self','Third Party') product_status," +
      "tacc.click_count " +
      "from tmp_area_click_count tacc join product_info pi on tacc.pid = pi.product_id"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")
  }

  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    val taskUUID = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setAppName("area").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


    // cityId2PidRDD : RDD[ cityId,productId]
    val cityId2PidRDD = getCityAndProductInfo(sparkSession, taskParam)

    // cityId2AreaInfoRDD: RDD[(cityId,CityAreaInfo)]
    val cityId2AreaInfoRDD = getCityAreaInfo(sparkSession)

    // tmp_area_basic_info : 表中的一条数据代表一次商品点击行为
    // 毕竟在得到这个表的过程中,没有进行聚合操作
    getAreaPidBasicInfoTable(sparkSession, cityId2PidRDD, cityId2AreaInfoRDD)


    sparkSession.udf.register("concat_long_string", (v1: String, v2: String, split: String) => {
      v1 + split + v2
    })

    sparkSession.udf.register("group_concat_distinct", new GroupConcatDistinct)

    // 创建 tmp_area_click_count临时视图
    getAreaProductClickCountTable(sparkSession)

    sparkSession.sql("select extend_info from product_info").show()

    sparkSession.udf.register("get_json_field", (json: String, field: String) => {
      val jsonObject = JSONObject.fromObject(json)
      jsonObject.getString(field)
    })

    getAreaProductClickCountInfo(sparkSession)

    getTop3Product(sparkSession,taskUUID)


  }

}
