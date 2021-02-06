import com.jtLiBrain.scalikejdbc.pool.provider.C3P0ConnectionPoolFactory
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scalikejdbc._

/**
 * http://scalikejdbc.org/documentation/connection-pool.html
 */
class ConnectionPoolProviderUsage extends FunSuite with BeforeAndAfterEach {
  val url = "jdbc:mysql://localhost:3306/test?useSSL=false"
  val user = "root"
  val password = "123456"

  override def beforeEach(): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
  }

  override def afterEach(): Unit = {
    ConnectionPool.closeAll()
  }

  test("by implicit variable") {
    implicit val factory = C3P0ConnectionPoolFactory
    ConnectionPool.singleton(url, user, password)
  }

  test("by explicitly specified") {
    // case 1: dbcp2 is the default pool factory
    ConnectionPool.singleton(url, user, password,
      ConnectionPoolSettings(connectionPoolFactoryName = "commons-dbcp2"))

    // case 2: use BONECP, it is built-in
    ConnectionPool.singleton(url, user, password,
      ConnectionPoolSettings(connectionPoolFactoryName = "bonecp"))

    // case 3: add C3P0 to scalikejdbc
    scalikejdbc.ConnectionPoolFactoryRepository.add("c3p0", C3P0ConnectionPoolFactory)

    ConnectionPool.singleton(url, user, password,
        ConnectionPoolSettings(connectionPoolFactoryName = "c3p0"))

    // case 4: use HikariCP as provider
    val dataSource: DataSource = {
      val ds = new HikariDataSource()
      ds.setDataSourceClassName("dataSourceClassName")
      ds.addDataSourceProperty("url", url)
      ds.addDataSourceProperty("user", user)
      ds.addDataSourceProperty("password", password)
      ds
    }
    ConnectionPool.singleton(new DataSourceConnectionPool(dataSource))
  }



}
