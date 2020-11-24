package myExer
import collection.mutable
import org.apache.spark.util.AccumulatorV2

class SessionAggrStatAccumulator extends AccumulatorV2 [String,mutable.HashMap[String,Int]]{

  // 保存所有聚合数据
  private val aggrStatMap = mutable.HashMap[String,Int]()

  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized{
      newAcc.value ++= this.aggrStatMap
    }
    newAcc
  }

  override def reset(): Unit = {
    aggrStatMap.clear
  }

  override def add(v: String): Unit = {
    if(!aggrStatMap.contains(v))
      aggrStatMap. += (v->0)
    aggrStatMap.update(v,aggrStatMap(v) + 1 )
  }

  // 各个task的累加器进行合并的方法
  // 合并另一个类型相同的累加器
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match{
      case acc:SessionAggrStatAccumulator =>{
        (this.aggrStatMap /:acc.value)(
          {
          case (map,(k,v))=> {
            map += (k -> (v + map.getOrElse(k, 0)))
          }
        }
      )

      }
    }
  }

  // 获取累加器的值
  // AccumulatorV2对外访问的数据结果
  override def value: mutable.HashMap[String, Int] ={
    this.aggrStatMap
  }
}
