package pe.com.claro.util

import java.util.{Calendar, _}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Delete, HBaseAdmin}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object UtilProcesamiento {


  def obtenerFechaInicial(queryDate: Date): String = {

    val calendar = Calendar.getInstance
    calendar.setTime(queryDate)
    val year: Int = calendar.get(Calendar.YEAR)

    ""
  }


  def obtenerFechaFinal(spark: SparkSession, env: String, nomTabla: String, id: String = "", isCtrlParticion: Boolean): DataFrame = {
    val dfCtrlInfo = spark.sql(s"select * from ${env}_cfiscal_ctrl.ctrl_particion_tabla")
    var df = spark.read.parquet(s"hdfs://hacluster/claro/cfiscal/${env}/cdv/data/${env}_cfiscal_cdv/preparation/output/${nomTabla}")
    if (isCtrlParticion) {
      val fecParticion = dfCtrlInfo.filter(col("nomtabla") === nomTabla).select("fecparticion").first()(0).toString
      df = df.filter(col("fch_corte_datos") === fecParticion).filter(col("num_ver_proc") === "0").filter(col("flg_acep") === "S")
    }
    if (id != "") df = df.filter(!col(s"${id}").isNull)
    df = df.dropDuplicates()
    df
  }


  def registrarCtrlInfo(spark: SparkSession, env: String, nomTabla: String, id: String = "", isCtrlParticion: Boolean): DataFrame = {
    val dfCtrlInfo = spark.sql(s"select * from ${env}_cfiscal_ctrl.ctrl_particion_tabla")
    var df = spark.read.parquet(s"hdfs://hacluster/claro/cfiscal/${env}/cdv/data/${env}_cfiscal_cdv/preparation/output/${nomTabla}")
    if (isCtrlParticion) {
      val fecParticion = dfCtrlInfo.filter(col("nomtabla") === nomTabla).select("fecparticion").first()(0).toString
      df = df.filter(col("fch_corte_datos") === fecParticion).filter(col("num_ver_proc") === "0").filter(col("flg_acep") === "S")
    }
    if (id != "") df = df.filter(!col(s"${id}").isNull)
    df = df.dropDuplicates()
    df
  }

  def truncateHbaseTable(spark: SparkSession, esquema: String, nombreTabla: String): Unit = {
    val configuration: Configuration = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)
    val admin = new HBaseAdmin(configuration)
    admin.disableTable(TableName.valueOf(s"${esquema}:${nombreTabla}"))
    admin.truncateTable(TableName.valueOf(s"${esquema}:${nombreTabla}"), true)
  }

  def saveCtrlInfo(esquema: String, dataFrame: DataFrame, keyCtrlInfo: String, yearField: String, monthField: String): Unit = {

    val dfCtrlInfo = dataFrame.groupBy(yearField, monthField).agg(lit("1") as "total")
      .withColumn("ctrl_desrubro", lit(keyCtrlInfo))
      .withColumn("ctrl_fecactualizacion", current_timestamp())
      .withColumn("key", concat(col("ctrl_desrubro"), lit("_"), col(yearField), lit("_"), col(monthField)))

    val reorderedColumnNames = Array(
      "key"
      , "ctrl_desrubro"
      , yearField
      , monthField
      , "ctrl_fecactualizacion"
    )

    val dfResult = dfCtrlInfo.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
    dfResult.write.insertInto(s"${esquema}_cfiscal_bdv.fact_ctrl_info")
  }

  def deleteKeyCtrlInfo(spark: SparkSession, env: String, keyName: String): Unit = {

    val configuration: Configuration = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)

    val tableName = s"${env}_cfiscal_bdv:fact_ctrl_info"
    val hbaseContext = new HBaseContext(spark.sparkContext, configuration)

    val deletedf = spark.sql(s"""select key from ${env}_cfiscal_bdv.fact_ctrl_info where ctrl_desrubro = "${keyName}"  """.stripMargin)

    val rdd = deletedf.rdd.map(r => (r.getString(0))) //rowkey

    hbaseContext.bulkDelete[String](rdd, TableName.valueOf(tableName), putRecord => new Delete(Bytes.toBytes(putRecord)), 10000)

  }

  def deleteKeyVista(spark: SparkSession, env: String, tableView: String,
                     campoTipoIndicador:String,valorTipoIndicador:String): Unit = {

    val configuration: Configuration = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)

    val tableName = s"${env}_cfiscal_bdv:${tableView}"
    val hbaseContext = new HBaseContext(spark.sparkContext, configuration)

    val deletedf = spark.sql(s"""select key from ${env}_cfiscal_bdv.${tableView} where ${campoTipoIndicador} = "${valorTipoIndicador}"  """.stripMargin)

    val rdd = deletedf.rdd.map(r => (r.getString(0))) //rowkey

    hbaseContext.bulkDelete[String](rdd, TableName.valueOf(tableName), putRecord => new Delete(Bytes.toBytes(putRecord)), 10000)

  }

}
