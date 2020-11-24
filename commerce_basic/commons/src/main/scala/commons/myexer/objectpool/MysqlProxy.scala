package commons.myexer.objectpool

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import commons.pool.QueryCallback

case class MysqlProxy(jdbcUrl : String,jdbcUser :String,jdbcPassword : String,client:Option[Connection] = None) {

  // 获取客户端连接对象
  private val mysqlClient = client getOrElse(
    DriverManager.getConnection(jdbcUrl,jdbcUser,jdbcPassword)
  )

  // 执行增删改sql语句

  def executeUpdate(sql:String,params:Array[Any])={
    var rtn = 0;
    var pstmt:PreparedStatement = null;
    try{
      // 1. 关闭自动提交
      mysqlClient.setAutoCommit(false)
      // 2. 根据传入到sql语句创建prepareStatement
      pstmt = mysqlClient.prepareStatement(sql);
      // 3. 为prepareStatement的每个参数填写数值
      if(params != null && params.length >0){
        for(i <- 0 until params.length){
          pstmt.setObject(i+1,params)
        }
      }
      // 4. 执行增删改操作
      rtn = pstmt.executeUpdate()
      // 5. 手动提交
      mysqlClient.commit()
    }catch{
      case e:Exception => e.printStackTrace()
    }
    rtn
  }

  def executeQuery(sql :String,params : Array[Any],queryCallback : QueryCallback)={
    var pstmt :PreparedStatement = null
    var rs :ResultSet = null

    try{
      // 1. 根据传入到sql创建prepareStatement
      val statement = mysqlClient.prepareStatement(sql)
      // 2. 为prepareStatement中的每个参数填写数值
      if(params != null && params.length > 0){
        for( i <- 0 until params.length){
          statement.setObject(i+1,params(i))
        }
      }
      // 3. 执行查询操作
      val rs = statement.executeQuery()
      // 4. 处理查询后的结果
      queryCallback.process(rs)
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }

  // 批量执行sql语句
  def executeBatch(sql:String,paramsList : Array[Array[Any]])={
    var rtn :Array[Int] = null
    var pstmt : PreparedStatement = null

    try{
      // 1. 关闭自动提交
      mysqlClient setAutoCommit(false)
      val statement = mysqlClient.prepareStatement(sql)
      // 2. 为prepareStatement中的每个参数填写数值
      if(paramsList != null && paramsList.length > 0 ){
        for(params <- paramsList){
          for( i<- 0 until params.length){
            statement.setObject(i+1,params(i))
          }
          statement.addBatch()
        }
      }
      // 3. 执行批量的sql语句
      rtn = statement.executeBatch
      // 4. 手动提交
      mysqlClient.commit()
    }catch{
      case e :Exception => e.printStackTrace()
    }
  }

  def shutdown ={
    mysqlClient.close()
  }

}
