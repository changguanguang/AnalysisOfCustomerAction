package commons.myexer.objectpool

import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}

object CreateMysqlPool {

  Class.forName("com.mysql.jdbc.Driver")

  private var genericObjectPool : GenericObjectPool[MysqlProxy] = null

  // 伴生对象
  def apply():GenericObjectPool[MysqlProxy]={

    // 单例模式
    if(this.genericObjectPool == null){
      this.synchronized{
        // 获取MySQL配置参数
        val jdbcUrl = ConfigurationManager.config.getString(Constants.JDBC_URL)
        val jdbcUser = ConfigurationManager.config.getString(Constants.JDBC_USER)
        val jdbcPassword = ConfigurationManager.config.getString(Constants.JDBC_PASSWORD)
        val size = ConfigurationManager.config.getInt(Constants.JDBC_DATASOURCE_SIZE)

        val pooledFactory = new PooledMysqlClientFactory(jdbcUrl,jdbcUser,jdbcPassword)
        val poolConfig = {
          val c = new GenericObjectPoolConfig
          c.setMaxTotal(size)
          c.setMaxIdle(size)
          c
        }
        // 对象池的创建需要工厂类和配置类
        // 返回一个GenericObjectPool对象池
        this.genericObjectPool = new GenericObjectPool[MysqlProxy](pooledFactory,poolConfig)

      }
    }
    genericObjectPool
  }
}
