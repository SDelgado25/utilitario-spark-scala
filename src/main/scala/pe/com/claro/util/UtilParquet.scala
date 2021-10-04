package pe.com.claro.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

object UtilParquet {


  def obtenerDataCDV_bkp(spark: SparkSession, env: String, nomTabla: String, id: String = "", isCtrlParticion: Boolean): DataFrame = {
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

  def obtenerDataCDV(spark: SparkSession, env: String, nomTabla: String, id: String = "", isCtrlParticion: Boolean): DataFrame = {

    import spark.implicits._
    val dfCtrlInfo = spark.sql(s"select * from ${env}_cfiscal_ctrl.ctrl_particion_tabla")
    val schemacdv = s"${env}_cfiscal_cdv"
    val rutahdfs = s"/claro/cfiscal/${env}/cdv/data/${env}_cfiscal_cdv/preparation/output/${nomTabla}" // hdfs://hacluster

    spark.sql(s"msck repair table ${schemacdv}.${nomTabla}")

    var df = if (isCtrlParticion) {
      if (existenciaCDV( rutahdfs)) {
        val df = spark.sql(s"""select * from ${schemacdv}.${nomTabla} """)
        df
      } else {
        val num_ver_proc = "0"
        val flg_acep = "S"
        val fecParticion = dfCtrlInfo.filter(col("nomtabla") === nomTabla).select("fecparticion").first()(0).toString

        var partdf = spark.read.parquet(s"${rutahdfs}/fch_corte_datos=${fecParticion}/num_ver_proc=${num_ver_proc}/flg_acep=${flg_acep}")
        partdf = partdf.
          withColumn("fch_corte_datos", lit(fecParticion)).
          withColumn("num_ver_proc", lit(num_ver_proc)).
          withColumn("flg_acep", lit(flg_acep))
        partdf
      }

    }
    else {
      if (existenciaCDV(rutahdfs)) {
        val df = spark.sql(s"""select * from ${schemacdv}.${nomTabla} """)
        df
      } else {
        var nopartdf = spark.read.parquet(s"${rutahdfs}")
        nopartdf = nopartdf.filter(col("num_ver_proc") === "0").filter(col("flg_acep") === "S")
        nopartdf
      }
    }

    if (id != "") df = df.filter(!col(s"${id}").isNull)
    // df = df.dropDuplicates()
    df
  }

  def existenciaCDV(rutahdfs: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val ruta = new Path(rutahdfs)
    var existenciaCDV: Boolean = false
    val ldirectory = fs.listStatus(ruta)

    if (fs.exists(ruta) && ldirectory.isEmpty) {
      existenciaCDV = true
    }

    existenciaCDV
  }

}
