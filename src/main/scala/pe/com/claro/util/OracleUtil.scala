package pe.com.claro.util

import java.sql.{Connection, DatabaseMetaData, DriverManager, SQLException}
import scala.util._


object OracleUtil {

  def loadDriverClass: Unit = {
    val driverClass = "oracle.jdbc.driver.OracleDriver"
    try {
      Class.forName(driverClass)
    } catch {
      case e: ClassNotFoundException => throw e
      case e: Exception => throw e
    }
  }

  def getConnection(jdbcUrl: String, usernameJdbc: String, passwordJdbc: String): Connection = {
    var connection: Connection = null
    Try {
      connection = DriverManager.getConnection(jdbcUrl, usernameJdbc, passwordJdbc)
      if (connection != null) println("Conexion exitosa a BD Oracle")
      else println("No se logro realizar la conexion a Oracle")
    } match {
      case Failure(exception) => {
        exception.printStackTrace
        // log.error("ERROR", exception)
      }
      case Success(_) => {
        // log.info("PROCESAMIENTO OK")
      }


    }
    connection
  }

  @throws(classOf[SQLException])
  def execInsertWithCommitAppend(conn: Connection, queryStatement: String): Unit = {
    conn.setAutoCommit(false)
    val statement = conn.createStatement
    statement.executeUpdate(queryStatement)
    conn.commit
    println(("Se inserto correctamente el registro"))
  }

  @throws(classOf[SQLException])
  def execTruncate(conn: Connection, tableTarget: String): Unit = {
    conn.setAutoCommit(false)
    val statement = conn.createStatement
    /*Trunca la tabla previo a la insercion de los nuevos registros*/
    statement.executeUpdate("TRUNCATE TABLE " + tableTarget)
    conn.commit
    println(("Se trunco correctamente la tabla"))
  }

  @throws(classOf[SQLException])
  def execDeleteByService(conn: Connection, tableTarget: String, campo_servicio: String, servicio: String): Unit = {
    conn.setAutoCommit(false)
    val statement = conn.createStatement
    /*Elimina los datos existentes previo a la insercion de los nuevos registros para determinado servicio*/
    statement.executeUpdate("DELETE FROM " + tableTarget + s" WHERE UPPER(${campo_servicio}) = '" + servicio + "'")
    conn.commit
    println((s"Se eliminaron los registros existentes en la tabla ${tableTarget} para el servicio ${servicio}"))
  }

  @throws(classOf[SQLException])
  def execInsertWithCommitOverwrite(conn: Connection, tableTarget: String, queryStatement: String): Unit = {
    conn.setAutoCommit(false)
    val statement = conn.createStatement
    /*Insercion con OverWrite*/
    statement.executeUpdate("TRUNCATE TABLE " + tableTarget)
    conn.commit
    statement.executeUpdate(queryStatement)
    conn.commit
    println(("Se inserto correctamente el registro"))
  }

  @throws(classOf[SQLException])
  def execCreateTable(conn: Connection, owner: String, tableTarget: String, createStatement: String, commentTable: String): Unit = {
    val statement = conn.createStatement
    val dbm: DatabaseMetaData = conn.getMetaData
    val tables = dbm.getTables(null, owner.toUpperCase(), tableTarget.toUpperCase(), null)
    if (tables.next()) {
      println(s"La tabla ${owner.toUpperCase()}.${tableTarget.toUpperCase()} ya existe")
    }
    else {
      println("La tabla no existe. Se ejecutara script de creacion de tabla")
      statement.execute(createStatement.toUpperCase())
      println(s"Se creo la tabla ${owner.toUpperCase()}.${tableTarget.toUpperCase()}")
      statement.execute(
        s"""COMMENT ON TABLE ${owner.toUpperCase()}.${tableTarget.toUpperCase()}
           | IS '${commentTable}'""".stripMargin)
    }
  }

  @throws(classOf[SQLException])
  def execCommentColumnTable(conn: Connection, owner: String, tableTarget: String, columnTable: String, commentColumn: String): Unit = {
    val statement = conn.createStatement
    statement.execute(
      s"""COMMENT ON COLUMN ${owner.toUpperCase()}.${tableTarget.toUpperCase()}.${columnTable.toUpperCase()}
         | IS '${commentColumn}'""".stripMargin)

  }

}
