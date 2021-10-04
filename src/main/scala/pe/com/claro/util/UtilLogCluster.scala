package pe.com.claro.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object UtilLogCluster {

  def Write_Log_process(spark: SparkSession, idprocess: String, log4jUtil: Log4jUtil, log: Logger): Unit = {

    val applicationID = spark.sparkContext.applicationId
    log.info(log4jUtil.generateTrazadoMessage(s"PROCESS_ID [ ${idprocess} ] APPLICATION_ID [ ${applicationID} ]"))
    val fs = FileSystem.get(new Configuration())
    log.info(log4jUtil.generateTrazadoMessage(s"Escritura del log en HDFS en la ruta /var/logs/${idprocess}"))
    val os = fs.create(new Path("/var/logs/" + idprocess))
    os.write(applicationID.getBytes)
  }
}
