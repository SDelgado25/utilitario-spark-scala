package pe.com.claro.util

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.util.Try

object UtilExport {
  def exportCSV(spark:SparkSession,df:DataFrame,ruta_temp:String,
                ruta_destino:String,nombre_archivo:String):Unit={
    val dataDF = df.select(df.columns.map(c => df.col(c).cast("string")): _*)
    import scala.collection.JavaConverters._
    val headerDF = spark.createDataFrame(List(Row.fromSeq(dataDF.columns.toSeq)).asJava, dataDF.schema)
    headerDF.union(dataDF).write.mode(SaveMode.Overwrite
    ).option("header", "false").option("delimiter","|"
    ).csv(ruta_temp)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    copyMerge(fs, new Path(ruta_temp),
      fs, new Path(s"${ruta_destino}/${nombre_archivo}.txt"),
      true, spark.sparkContext.hadoopConfiguration)
  }

  def copyMerge(
                 srcFS: FileSystem, srcDir: Path,
                 dstFS: FileSystem, dstFile: Path,
                 deleteSource: Boolean, conf: Configuration
               ): Boolean = {

    if (dstFS.exists(dstFile))
      throw new IOException(s"Target $dstFile already exists")

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {

      val outputFile = dstFS.create(dstFile)
      Try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile() =>
              val inputFile = srcFS.open(status.getPath())
              Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()

      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }

}
