package commons.myexer.objectpool

import java.sql.Connection

import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

class PooledMysqlClientFactory (jdbcUrl :String,jdbcUser:String,jdbcPassword:String,
                                client:Option[Connection]=None) extends BasePooledObjectFactory[MysqlProxy] with Serializable {
  // 用于池来创建对象
  override def create(): MysqlProxy = {
    MysqlProxy(jdbcUrl,jdbcUser,jdbcPassword,client)
  }

  // 用于池来包装对象
  override def wrap(obj: MysqlProxy): PooledObject[MysqlProxy] = {
    new DefaultPooledObject(obj)
  }

  // 用于池来销毁对象
  override def destroyObject(p: PooledObject[MysqlProxy]): Unit = {
    p.getObject.shutdown
    super.destroyObject(p)
  }
}


