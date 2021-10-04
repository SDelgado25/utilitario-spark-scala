package pe.com.claro.util

import java.io.File

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object UtilJob {

  val log: Logger = Logger.getLogger(UtilJob.getClass)

  def readCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("com.databricks.spark.csv").load(path)
  }

  def readParquet(spark: SparkSession, parquetPath: String): DataFrame = {
    spark.read.parquet(parquetPath)
  }

  def readOrc(spark: SparkSession, orcPath: String): DataFrame = {
    spark.read.format("orc").load(orcPath)
  }

  def readAvro(spark: SparkSession, rutas: List[String], schema: String): DataFrame = {
    spark.read.format("com.databricks.spark.avro").option("avroSchema", schema).load(rutas: _*)
  }

  def getAvroSchema(spark: SparkSession, path: String): String = {
    val schema = new Schema.Parser().parse(new File(path))
    schema.toString()
  }

  private def getHadoopPath(): FileSystem = {
    val hadoopConf = new Configuration()
    val hadoopFS = FileSystem.get(hadoopConf)
    hadoopFS
  }

  def deleteHadoopPathIfExists(path: String): Unit = {
    val hadoopFS = getHadoopPath()
    val hadoopPath = new Path(path)
    if (hadoopFS.exists(hadoopPath)) {
      hadoopFS.delete(hadoopPath, true)
    }
  }

  def getPartitionsFromPath(path: String, filterContains: String): Array[String] = {
    val filter: PathFilter = new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.contains(filterContains)
      }
    }

    val hadoopPath = new Path(path)
    getHadoopPath().listStatus(hadoopPath, filter).map(fs => {
      fs.getPath.getName
    })
  }

  def getExistingPartitionsFromPath(path: String): Array[String] = {
    val hadoopPath = new Path(s"${path}")
    getHadoopPath().listStatus(hadoopPath).map(fs => {
      fs.getPath.getName
    })

  }

  def moveExistingToTarget(spark: SparkSession, srcBasePath: String, trgBasePath: String, partitions: List[String]): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.mkdirs(new Path(trgBasePath))
    for (existingPartitions <- partitions) {
      var srcHDFSPath = new Path(srcBasePath + existingPartitions)
      var trgHDFSPath = new Path(trgBasePath + existingPartitions)
      fs.rename(srcHDFSPath, trgHDFSPath)
    }
  }

  def writeParquet(parquetDF: DataFrame, path: String, partition: String, saveMode: String): Unit = {
    parquetDF.write.mode(saveMode).partitionBy(partition).parquet(path)
  }

  def writeOrc(orcDF: DataFrame, path: String, partitionCols: Seq[String], saveMode: String): Unit = {
    orcDF.write.mode(saveMode).partitionBy(partitionCols: _*).format("orc").save(path)
  }

  def writeAvro(spark:SparkSession,avroDF: DataFrame, path: String, partitionCols: Seq[String], modo: String,schema:String): Unit = {
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    avroDF.write.partitionBy(partitionCols: _*
    ).format("com.databricks.spark.avro"
    ).mode(SaveMode.Overwrite
    ).option("forceSchema", schema
    ).save(path)
  }

  def dataProcJob(appName: String): String = {
    val charSplit: String = "/"
    val numSplit: Int = 3
    if (appName.contains(charSplit)) {
      appName.split(charSplit)(numSplit)
    }
    else {
      appName
    }
  }

  def filterData(df: DataFrame, filtro: String): DataFrame = {
    val dfTmp = df.dropDuplicates()
    val dfResult = dfTmp.filter(filtro)
    dfResult
  }

  def getSingleStringfromColDF(spark: SparkSession, df: DataFrame, columnName: String): String = {
    // val strColumnValue= df.select(columnName).map(r => r.getString(0)).collect.toList
    val col_val_df = df.select(columnName).collect()
    val col_val_str = col_val_df.map(x => x.get(0)).mkString(",")
    col_val_str
  }

  def castStringColumnToDate(spark: SparkSession, columnStr: Column, fmtOrigen: String): Column = {
    when(to_date(trim(columnStr), s"${fmtOrigen}").isNotNull,
      to_date(trim(columnStr), s"${fmtOrigen}"))
  }

  def castStringColumnMultipleFormatToDate(spark:SparkSession,columnStr:Column,
                                           fmtOrigen1:String,fmtOrigen2:String): Column =
  {
    when(to_date(trim(columnStr),s"${fmtOrigen1}").isNotNull,
      to_date(trim(columnStr),s"${fmtOrigen1}")).when(
      to_date(trim(columnStr),s"${fmtOrigen2}").isNotNull,
      to_date(trim(columnStr),s"${fmtOrigen2}")
    ).when(
      ((columnStr.cast("double")- 25568) * 86400).cast("timestamp").isNotNull,
      ((columnStr.cast("double")- 25568) * 86400).cast("timestamp").cast("date")
    )
  }

  def castStringColumnToTimestamp(spark: SparkSession, columnStr: Column, fmtOrigen: String): Column = {
    unix_timestamp(columnStr, fmtOrigen).cast(TimestampType)
  }

  def getUniqueIncrementalID(spark: SparkSession, dfInput: DataFrame, colTarget: String): DataFrame = {
    val rddWithId = dfInput.rdd.zipWithIndex
    val newSchema = StructType(dfInput.schema.fields ++ Array(StructField(s"${colTarget}", LongType, false)))
    val dfZippedWithId = spark.createDataFrame(rddWithId.map { case (row, index) => Row.fromSeq(row.toSeq ++ Array(index + 1)) }, newSchema)
    dfZippedWithId
  }

  def getArrayStringfromColDF(spark: SparkSession, df: DataFrame, columnName: String): Array[Any] = {
    // val strColumnValue= df.select(columnName).map(r => r.getString(0)).collect.toList
    val col_val_df = df.select(columnName).collect()
    val col_val_str = col_val_df.map(x => x.get(0))
    col_val_str
  }

  def splitPorDelimitador(columnInput: Column, sep: String, i: String = ""): Column = {
    if (!i.isEmpty) {
      split(columnInput, s"[${sep}]").getItem(i.toInt)
    }
    else {
      split(columnInput, s"[${sep}]")
    }
  }

  def eliminaTablasTemporalesConCoincidencia(spark: SparkSession, esquema: String, patron: String): Unit = {
    val dfTabTempListDrop = spark.sql(
      s"show tables in ${esquema} like '${patron}'")
    val arrayTabsDrop = dfTabTempListDrop.collect().map(
      value => value(1).toString).distinct
    arrayTabsDrop.foreach(tabla => {
      spark.sql(s"DROP TABLE IF EXISTS ${esquema}.${tabla}")
    })
  }

  def genDFExcelFileSinSchema(spark:SparkSession,
                              sheets:String="",locale:String="",
                              skipHeader:Boolean=false,numLinesToSkip:Int=0,
                              skipLinesInAllSheets:Boolean=false,
                              datePattern:String="yyyy-MM-dd",
                              dateTimePattern:String="yyyy-MM-dd HH:mm:ss.0",
                              decimalFormat:String="0000000000000.0000",simpleMode:Boolean,
                              inferSchema:Boolean,numLinesInferSchema:String="-1",
                    filename:String):DataFrame={
    var numLinesInferSchemaCalc=numLinesInferSchema
    if(inferSchema==false) numLinesInferSchemaCalc="0"
    val sqlContext = spark.sqlContext
    val df = sqlContext.read
      .format("org.zuinnote.spark.office.excel"
      ).option("read.spark.simpleMode",simpleMode
    ).option("hadoopoffice.read.locale.bcp47",locale
    ).option("read.spark.simpleMode.maxInferRows",numLinesInferSchemaCalc
    ).option("hadoopoffice.read.sheets",sheets
    ).option("hadoopoffice.read.header.skipheaderinallsheets",skipHeader
    ).option("hadoopoffice.read.sheet.skiplines.num",numLinesToSkip
    ).option("hadoopoffice.read.sheet.skiplines.allsheets",skipLinesInAllSheets
    ).option("hadoopoffice.read.simple.datePattern",datePattern
    ).option("hadoopoffice.read.simple.dateTimePattern",dateTimePattern
    ).option("hadoopoffice.read.simple.decimalFormat",decimalFormat
    ).load(filename)
    df
  }

  def obtieneCtrlCruce(df:DataFrame,num_paso:String,ident_proceso:String):DataFrame={
    val dfControl=df.select(col("*"), lit(num_paso).as("num_paso"),
      lit(ident_proceso).as("ident_proceso")
    ).groupBy("num_paso", "ident_proceso").agg(
      count("*").as("cant_registros"),
      round ((sum(when(col("id").isNull, 0).otherwise(1)
      ) * 100 / count("*")),4).alias("porc_cruce"))
    dfControl
  }

  def registraAlertasObtOrigen(spark:SparkSession,log:Logger,
                               log4jUtil: Log4jUtil,
                               tabla_input:String,
                               tabla_alertas:String,
                               params_alerta:String,
                               tipo:String,paso:String,
                               df:DataFrame):Unit={
    if(df.count==0){
      val mensaje_alerta="El paso de obtencion del DF Origen no devuelve registros"
      registraMsjAlerta(spark,tabla_alertas,params_alerta,tipo,paso,mensaje_alerta)
      log.error(log4jUtil.generateTrazadoMessage(
        s"Lectura de tabla ${tabla_input} no devolvio registros"))
    }
  }

  def registraMsjAlerta(spark:SparkSession,tabla_alertas:String,
                        params_alerta:String,
                        tipo:String,paso:String, mensaje_alerta:String):Unit={
    val arrayParamsAlerta=params_alerta.split("#")
    val fase=arrayParamsAlerta(0)
    val capa=arrayParamsAlerta(1)
    val clase=arrayParamsAlerta(2)
    val idtransaccion=arrayParamsAlerta(3)
    val tabla_destino=arrayParamsAlerta(4)
    spark.sql(s"INSERT OVERWRITE TABLE ${tabla_alertas} " +
      s"PARTITION (fase='${fase}', capa='${capa}', clase='${clase}'," +
      s"idtransaccion='${idtransaccion}',paso='${paso}') " +
      s"VALUES('${tabla_destino}','${tipo}','${mensaje_alerta}',current_timestamp() ) ")
  }

  def withColumnsNullString(cols: Seq[String], df: DataFrame) = {
    cols.foldLeft(df)((df, c) => df.withColumn(c, lit(null: String).cast(StringType)))
  }

}
