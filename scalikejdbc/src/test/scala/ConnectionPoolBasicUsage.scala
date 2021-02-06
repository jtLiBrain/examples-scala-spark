import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scalikejdbc._

/**
 * http://scalikejdbc.org/documentation/connection-pool.html
 */
class ConnectionPoolBasicUsage extends FunSuite with BeforeAndAfterEach {
  val poolName = "foo"

  val url = "jdbc:mysql://localhost:3306/test?useSSL=false"
  val user = "root"
  val password = "123456"

  override def beforeEach(): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
  }

  private def registersGlobalPool(url: String, user: String, password: String): Unit = {
    ConnectionPool.singleton(url, user, password)
  }

  private def registersNamedPool(url: String, user: String, password: String): Unit = {
    val settings = ConnectionPoolSettings(
      initialSize = 5,
      maxSize = 20,
      connectionTimeoutMillis = 3000L,
      validationQuery = "select 1 from dual")

    // all the connections are released, old connection pool will be abandoned
    ConnectionPool.add(poolName, url, user, password, settings)
  }

  override def afterEach(): Unit = {
    ConnectionPool.closeAll()
  }

  test("borrow a connection from pool") {
    // default
    val conn1: java.sql.Connection = ConnectionPool.borrow()

    // named
    val conn2: java.sql.Connection = ConnectionPool('named).borrow()
  }

  test("borrow and return a connection from default pool") {
    // after loading JDBC drivers
    registersGlobalPool(url, user, password)

    // case 1
    using(ConnectionPool.borrow()) { conn =>
      // do something
    }

    // case 2
    using(DB(ConnectionPool.borrow())) { db =>
      // ...
    }

    // case 3
    using(DB(ConnectionPool.borrow())) { db =>
      db.readOnly { implicit session =>
        // ...
      }
    }

    // case 4: equivalent to case 3
    DB readOnly { implicit session =>
      // ...
    }
  }

  test("borrow and return a connection from named pool") {
    // after loading JDBC drivers
    registersNamedPool(url, user, password)

    // case 1
    using(ConnectionPool(poolName).borrow()) { conn =>
      // do something
    }

    // case 2
    using(DB(ConnectionPool(poolName).borrow())) { db =>
      // ...
    }

    // case 3
    using(DB(ConnectionPool(poolName).borrow())) { db =>
      db.readOnly { implicit session =>
        // ...
      }
    }

    // case 4: equivalent to case 3
    NamedDB(poolName) readOnly { implicit session =>
      // ...
    }
  }

  test("reuse DB instance") {
    // after loading JDBC drivers
    registersGlobalPool(url, user, password)

    val name = ""
    val id = ""

    // case 1: you can't do like this
    /*
    using(ConnectionPool.borrow()) { conn: java.sql.Connection =>
      val db: DB = DB(conn)

      db.localTx { implicit session =>
        sql"update something set name = ${name} where id = ${id}".update.apply()
      } // localTx or other APIs always close the connection to avoid connection leaks

      // the Connection already has been closed here!
      // java.sql.SQLException will be thrown!
      db.localTx { implicit session =>
        // ....
      }
    }
    */

    // case 2: you should do like this
    using(ConnectionPool.borrow()) { conn: java.sql.Connection =>
      val db: DB = DB(conn)

      // set as auto-close disabled
      db.autoClose(false)

      db.localTx { implicit session =>
        sql"update something set name = ${name} where id = ${id}".update.apply()
      } // localTx won't close the current Connection

      // this block also works fine!
      db.localTx { implicit session =>
        // ....
      }
    }
  }


}
