package request1

case class SortKey(clickCount:Long,orderCount:Long,payCount:Long) extends Ordered[SortKey]{
  override def compare(that: SortKey): Int = {
    if(this.clickCount - that.clickCount != 0){
      return (this.clickCount - that.clickCount).toInt
    }else if(this.orderCount - that.orderCount != 0){
      return (this.orderCount - that.orderCount).toInt
    }else{
      return (this.payCount - that.payCount).toInt
    }
  }
}
