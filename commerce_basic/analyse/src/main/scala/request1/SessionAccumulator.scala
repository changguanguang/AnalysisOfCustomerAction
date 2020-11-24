package request1

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Int]] {

  val countMap = new mutable.HashMap[String,Int]()
  override def isZero: Boolean ={
    countMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] ={

    val acc = new SessionAccumulator
    //++= 不改变这个对象位置,只是这个对象内部的数据发生变化
    acc.countMap ++= this.countMap
    acc
  }

  override def reset(): Unit = {
    this.countMap.clear()
  }

  override def add(v: String): Unit = {

    // override | addNew
    countMap.put(v,countMap.getOrElse(v,0) +1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc : SessionAccumulator => acc.countMap.foldLeft(this.countMap){
        case (map,(k,v)) =>{
          map += (k -> (map.getOrElse(k,0)+v))
        }
      }
    }
  }
  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
