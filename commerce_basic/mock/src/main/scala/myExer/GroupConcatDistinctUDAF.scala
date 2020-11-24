package myExer

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

class GroupConcatDistinctUDAF  extends UserDefinedAggregateFunction {
  // 聚合函数输入参数类型
  override def inputSchema: StructType = StructType(StructField("cityInfo",StringType):: Nil)

  override def bufferSchema: StructType = {
    StructType(StructField("cityInfo",StringType)::Nil)
  }

  // 函数返回值的数据类型
  override def dataType: DataType = {
    StringType
  }

  // 一致性检验，如果为true，那么输入不变的情况下结果也是不变的
  override def deterministic: Boolean = {
    true
  }

  /**
    * 设置聚合中间的buffer的初始值
    *  两个初始buffer合并之后也应该是初始值
    *   0+0=0 √
    *   1+1=2  x
    *   "" + "" = ""
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 这里是字符串变量（常量），操作字符串后，对象会发生变化
    var bufferCityInfo = buffer.getString(0)

    val cityInfo = input.getString(0)

    if(!bufferCityInfo.contains(cityInfo)){
      if("".equals(bufferCityInfo))
        bufferCityInfo += cityInfo
      else
        bufferCityInfo += "," + cityInfo
      buffer.update(0,bufferCityInfo)
    }

  }
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0)
    val bufferCityInfo2 = buffer2.getString(0)

    for(cityInfo <- bufferCityInfo2.split(",")){
      if(!bufferCityInfo1.contains(cityInfo)){
        if("".equals(bufferCityInfo1)){
          bufferCityInfo1 += cityInfo
        }else{
          bufferCityInfo1 += "," + cityInfo
        }
      }
      buffer1.update(0,bufferCityInfo1)
    }
  }

  // 计算并返回最终聚合结果
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
