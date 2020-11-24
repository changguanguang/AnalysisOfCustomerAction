package request3

/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 10/28/17 12:38 PM.
 * Author: wuyufei.
 */


case class CityClickProduct(city_id:Long,
                                click_product_id:Long)

case class CityAreaInfo(city_id:Long,
                          city_name:String,
                          area:String)

//***************** 输出表 *********************

/**
  *
  * @param taskid
  * @param area
  * @param areaLevel
  * @param productid
  * @param cityInfos
  * @param clickCount
  * @param productName
  * @param productStatus
  */
case class AreaTop3Product(taskid:String,
                           area:String,
                           areaLevel:String,
                           productid:Long,
                           cityInfos:String,
                           clickCount:Long,
                           productName:String,
                           productStatus:String)

case class AreaTop3Product2(
                           area:String,
                           area_level:String,
                           city_infos:String,
                           pid:Long,
                           product_name:String,
                           product_status:String,
                           click_count:Long)