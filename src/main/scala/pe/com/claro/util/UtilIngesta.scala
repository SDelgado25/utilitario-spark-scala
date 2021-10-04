package pe.com.claro.util

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UtilIngesta {

  def validaArchivosPendientes(spark:SparkSession,df:DataFrame,env:String,codorigen:String):DataFrame={
    import spark.implicits._
    val esqCtrl=s"${env}_cfiscal_ctrl"
    val tabDestino = "ctrl_archivos_ingesta"
    val dfListActCtrlRutas = spark.sql(s"select * from ${esqCtrl}.${tabDestino}" +
      s" where codorigen='${codorigen}' and statusingesta='S' ")

    var dfJoinTmpActCtrlRutas = df.alias("a"
    ).join(dfListActCtrlRutas.alias("b"),
      col("a.codorigen") === col("b.codorigen") &&
        col("a.ruta") === col("b.desrutaarc") &&
        col("a.filename") === col("b.nomarchivo"),
      "left"
    ).select(
      $"a.*",
      $"b.fecmodificacion".as("fecmodifctrl")
    )
    dfJoinTmpActCtrlRutas = dfJoinTmpActCtrlRutas.withColumn("flgcondfecmodif",
      when(
        UtilJob.castStringColumnToTimestamp(
          spark, col("fecmodificacion"), "dd/MM/yyyy HH:mm:ss"
        ) > coalesce(
          col("fecmodifctrl"), lit("1900-01-01 00:00:00").cast("timestamp")
        ), "S"
      ).otherwise("N")
    )
    dfJoinTmpActCtrlRutas = dfJoinTmpActCtrlRutas.filter(col("flgcondfecmodif") === "S")
    dfJoinTmpActCtrlRutas
  }

  def validaAdjuntosPendientes(spark:SparkSession,df:DataFrame,env:String,codorigen:String):DataFrame={
    import spark.implicits._
    val esqCtrl=s"${env}_cfiscal_ctrl"
    val tabDestino = "ctrl_archivos_ingesta_email"
    val dfListActCtrlRutas = spark.sql(s"select * from ${esqCtrl}.${tabDestino}" +
      s" where codorigen='${codorigen}' and statusingesta='S' ")

    var dfJoinTmpActCtrlRutas = df.alias("a"
    ).join(dfListActCtrlRutas.alias("b"),
      col("a.codorigen") === col("b.codorigen") &&
        col("a.ruta") === col("b.desrutaarc") &&
        col("a.filename") === col("b.nomarchivo"),
      "left"
    ).select(
      $"a.*",
      $"b.fecmodificacion".as("fecmodifctrl")
    )
    dfJoinTmpActCtrlRutas = dfJoinTmpActCtrlRutas.withColumn("flgcondfecmodif",
      when(
        UtilJob.castStringColumnToTimestamp(
          spark, col("fecmodificacion"), "dd/MM/yyyy HH:mm:ss"
        ) > coalesce(
          col("fecmodifctrl"), lit("1900-01-01 00:00:00").cast("timestamp")
        ), "S"
      ).otherwise("N")
    )
    dfJoinTmpActCtrlRutas = dfJoinTmpActCtrlRutas.filter(col("flgcondfecmodif") === "S")
    dfJoinTmpActCtrlRutas
  }

  def generateDFExcelDataTXT(spark:SparkSession,
                             ruta:String,filename:String,delimiter:String):DataFrame={
    import spark.implicits._
    var df = spark.read.format("com.databricks.spark.csv"
    ).option("wholeFile", true
    ).option("multiline", true
    ).option("header", "false"
    ).option("inferSchema", "false"
    ).option("delimiter", "~"
    ).option("encoding", "ISO-8859-1"
    ).option("charset", "ISO-8859-1"
    ).option("mode","permissive"
    ).option("quote", "\""
    ).option("escape", "\""
    ).load(s"file://${ruta}/${filename}")

    val columna0=df.columns(0)
    df=df.select(
      col(columna0).as("data"),
      split(col(columna0),delimiter).as("data_split")
    )
    df=df.withColumn("size_split",
      size(col("data_split"))
    )
    val maxNumberCols=df.agg(max("size_split")).head()
    val maxNumberColsVal = maxNumberCols.getInt(0)
    val selectExprs = 0 until maxNumberColsVal map (
      i => $"temp".getItem(i).as(s"col$i")
      )

    df=df.withColumn("temp",
      split($"data", delimiter)).select(selectExprs:_*
    ).dropDuplicates()
    df

  }

  def agregaColsDFCtrlIngestaFiles(spark:SparkSession,
                              df:DataFrame,filename:String,flgFecArc:Boolean):DataFrame={
    var dfResult=df.withColumn("fecha_carga",current_date())
    val nom_archivo=filename.split("\\.")(0)
    dfResult=dfResult.withColumn("nom_archivo",lit(nom_archivo))
    if(flgFecArc==true)
      {
        val fecha_archivo=nom_archivo.split("_").last
        dfResult=dfResult.withColumn("fecha_archivo",lit(fecha_archivo))
      }
    dfResult
  }

  def registraCtrlArchivosFilesDF(spark:SparkSession,env:String,
                                 codorigen:String,desrutaarc:String,nomarchivo:String
                                ):Unit={
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    var dfCtrlFilesIng=spark.sql(s"select * from ${env}_cfiscal_ctrl.ctrl_archivos_ingesta_files "+
      s"where codorigen='${codorigen}' "+
      s"and desrutaarc='${desrutaarc}' "+
      s"and nomarchivo='${nomarchivo}' "+
      s"and fecingesta is null ")
    dfCtrlFilesIng=dfCtrlFilesIng.withColumn("fecingesta", current_date())
    dfCtrlFilesIng=dfCtrlFilesIng.withColumn("statusingesta", lit("S"))
    dfCtrlFilesIng.createOrReplaceTempView("tmpCtrlFilesIng")

    dfCtrlFilesIng.dropDuplicates.write.mode(SaveMode.Overwrite
    ).insertInto(s"${env}_cfiscal_ctrl.ctrl_archivos_ingesta_files")
  }

  def obtieneCodOperacion(df:DataFrame):DataFrame={
    val dfOut = df.withColumn("cod_unico_operacion",
      concat(
        coalesce(col("registro_sap"),lit("")),
        coalesce(year(col("fec_contable")),lit("")),
        coalesce(col("id_detallado"),lit(""))
      ))
    dfOut
  }

  def obtieneColDupRef(spark:SparkSession,df:DataFrame):DataFrame={
    var dfResult=df.withColumn("tmp_group",
      concat_ws("_",
        trim(year(col("fec_contable"))), //año contable
        trim(col("referencia"))
      )
    )
    val ws = Window.partitionBy("tmp_group").orderBy("id_detallado")
    dfResult =  dfResult.withColumn("rn", row_number.over(ws))
    dfResult =  dfResult.withColumn("duplicidad_referencia",
      when(col("rn")>1,"X")).drop("tmp_group","rn")
    dfResult
  }

  def obtieneDFAgrup(spark:SparkSession,df:DataFrame,
                       campoAgrupacion:String,
                       campoInput:String,
                       campoDestino:String,
                       opCalculo:String):DataFrame= {
    import spark.implicits._
    val dfResult = df.groupBy($"${campoAgrupacion}"
    ).agg(expr(s"${opCalculo}(${campoInput})").as(campoDestino)
    )

    dfResult
  }
}
