package pe.com.claro.util

import java.io.File
import java.text.SimpleDateFormat
import java.util._
import better.files.{File => ScalaFile}
import com.jcraft.jsch._
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.sql._
import scala.collection.JavaConversions._
import java.io.{FileInputStream, InputStream}
import java.io.BufferedInputStream
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorStreamFactory}
import org.apache.commons.compress.archivers.{ArchiveInputStream, ArchiveStreamFactory}

object UtilRutas {

  val log: Logger = Logger.getLogger(UtilJob.getClass)

  def generaFileListRutaFS(spark: SparkSession,
                           dfSelConfig: DataFrame,
                           esquemaDestino: String, tablaDestino: String,
                           env: String, log4jUtil: Log4jUtil
                          ): Unit = {
    println(s"Inicia metodo generaFileListRutaFS")
    val arrayRuta = dfSelConfig.collect().map(
      value => value(0).toString).distinct
    val arrayExtensions = dfSelConfig.collect().map(
      value => value(1).toString).distinct
    val tipo_conex = dfSelConfig.collect().map(
      value => value(2).toString).distinct(0)
    val flgrecursivo = dfSelConfig.collect().map(
      value => value(3).toString).distinct(0)
    val recursividad = if (flgrecursivo == "S") true else false
    val rutaregex = dfSelConfig.collect().map(
      value => value(4).toString).distinct(0)
    val archregex = dfSelConfig.collect().map(
      value => value(5).toString).distinct(0)
    val flgCompresion = dfSelConfig.collect().map(
      value => value(6).toString).distinct(0)

    /*arrayRuta.filter(_.matches(rutaregex)).map(
      ruta => {
        println(s"Listando archivos en ruta:${ruta}")
        val ruta_list = FileUtils.listFiles(
          new File(ruta), arrayExtensions, recursividad).toArray().filter(
          f => f.asInstanceOf[File].getName.matches(archregex)
        )
        println(s"Cantidad de archivos: ${ruta_list.size}")
        val sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
        //sdf.format(file.lastModified())
        val newArray = ruta_list.map(file => (
          file.asInstanceOf[File].getName,
          file.asInstanceOf[File].getParent,
          sdf.format(file.asInstanceOf[File].lastModified()),
          ruta
        )
        )
        //.substring(f.getAbsolutePath().lastIndexOf("\\")+1)
        import spark.implicits._
        var dfFilenames = spark.sparkContext.parallelize(
          newArray).toDF
        println(s"Cantidad de registros en df: ${dfFilenames.count()}")
        dfFilenames = dfFilenames.withColumn(
          "filename",
          col("_1")
        )
        dfFilenames = dfFilenames.withColumn(
          "ruta",
          col("_2")
        )
        dfFilenames = dfFilenames.withColumn(
          "fecmodificacion",
          col("_3")
        )
        dfFilenames = dfFilenames.withColumn(
          "desrutacfg",
          col("_4")
        ).drop("_1", "_2", "_3", "_4")

        dfFilenames.createOrReplaceTempView("tmpListFilenames")
        dfFilenames.show(10, false)
        spark.sql(s"DROP TABLE IF EXISTS ${esquemaDestino}.${tablaDestino}")
        spark.sql(
          s"""CREATE  TABLE IF NOT EXISTS ${esquemaDestino}.${tablaDestino}
             |stored as ORC
             |location '/claro/cfiscal/${env}/global/data/${esquemaDestino}/${tablaDestino}/'
             |select * from tmpListFilenames """.stripMargin)
      }
    )*/

    import spark.implicits._

    arrayRuta.map(
      ruta => {
        println(s"Listando archivos en ruta:${ruta}")
        val ruta_list = FileUtils.listFiles(
          new File(ruta), arrayExtensions, recursividad).toArray().filter(
          f => {
            //println(s"Nombre de archivo:${f.asInstanceOf[File].getName}")
            //println(s"Patron a evaluar:${archregex}")
            //println(s"Coincide con patron:${f.asInstanceOf[File].getName.matches(archregex)}")
            f.asInstanceOf[File].getName.matches(archregex)
          }
        )


        /*21/09: Valida si devolvio algun resultado luego de filtrar por patron en archivo*/
        if (ruta_list.size == 0) {
          log.error(log4jUtil.generateTrazadoMessage(
            s"No existen archivos coincidentes en la ruta" +
              s" para el patron: ${archregex} "))
          log.error("Se finaliza la ejecucion con codigo error:20")
          sys.exit(20)
          spark.stop()
          spark.close()
        }

        println(s"Cantidad de archivos: ${ruta_list.size}")
        val sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
        val contenidoArray = ruta_list.map(file => { //println(s"Listando archivos en file:${file}")
          (file.asInstanceOf[File].getName, file.asInstanceOf[File].getParent, sdf.format(file.asInstanceOf[File].lastModified()), ruta)
          //println(s"Listando archivos en getName:${file.asInstanceOf[File].getName}")
          //println(s"Listando archivos en getParent:${file.asInstanceOf[File].getParent}")
          //println(s"Listando archivos en lastModified:${sdf.format(file.asInstanceOf[File].lastModified())}")
          //println(s"Listando archivos en ruta:${ruta}")
        })

        val matchRegArray = contenidoArray.filter(f => f._2.matches(rutaregex)) //.map(x => {println(s"${x}")})

        var conteo = 0
        if (flgCompresion == "S") {
          matchRegArray.map { f =>
            println(s"ruta:" + f._2)
            println(s"archivo:" + f._1)
            conteo += UtilRutas.countFilesFromCompressedFile(f._2,f._1,".*_DP10_Atendido_.*","tar.gz")
          }
        }


        val dfFilenames = spark.sparkContext.parallelize(matchRegArray).toDF("filename", "ruta", "fecmodificacion", "desrutacfg")
        if (dfFilenames.count() > 0) {
          println(s"Creacion del DF  dfFilenames como tempView ruta :: ${ruta}")
          dfFilenames.createOrReplaceTempView("tmpListFilenames")
          //cargaSustentoDF.show(10, false)

          spark.sql(s"DROP TABLE IF EXISTS ${esquemaDestino}.${tablaDestino}")
          spark.sql(
            s"""CREATE  TABLE IF NOT EXISTS ${esquemaDestino}.${tablaDestino}
               |stored as ORC
               |location '/claro/cfiscal/${env}/global/data/${esquemaDestino}/${tablaDestino}/'
               |select t.*, '${conteo}' as cantidadarchivos from tmpListFilenames t """.stripMargin
          )
        } else {
          println(s"DF  dfFilenames Vacio de la ruta :: ${ruta}")
          /*21/09: Valida si devolvio algun resultado luego de filtrar por patron en archivo*/
          log.error(log4jUtil.generateTrazadoMessage(
            s"No existen rutas que coincidan con el patron ${rutaregex} "))
          log.error("Se finaliza la ejecucion con codigo error:21")
          sys.exit(21)
          spark.stop()
          spark.close()
        }

      })
  }

  def generaFileListRutaSFTP(spark: SparkSession, dfSelConfigSFTP: DataFrame, esquemaDestino: String, tablaDestino: String,
                             env: String, log4jUtil: Log4jUtil
                             //,arrayDesruta: Array[String], arrayExtensiones: Array[String], arrayParamsFTP: Array[String],nomarchivo: String, ruta_destino: String
                            ): Unit = {
    import spark.implicits._


    /* Ruta Origen de archivos*/
    val pathRoot = dfSelConfigSFTP.collect().map(value => value(0).toString).distinct(0) //"/home/csftp/SFTP_CLARO"
    /*  Obtención de lista de extensiones existentes     */
    val arrayExtensions = dfSelConfigSFTP.collect().map(value => value(1).toString).distinct

    val recursividad = dfSelConfigSFTP.collect().map(value => value(3).toString).distinct(0)
    /*Parametros de conexion SFTP */
    val SFTPHost = dfSelConfigSFTP.collect().map(value => value(4).toString).distinct(0) //"170.231.81.164"
    val SFTPUser = dfSelConfigSFTP.collect().map(value => value(5).toString).distinct(0) //"CLARO"
    val SFTPPass = dfSelConfigSFTP.collect().map(value => value(6).toString).distinct(0) //"CLARO*$"
    val SFTPPort = 22

    /* conexion SFTP */
    val jsch = new JSch()
    val session = jsch.getSession(SFTPUser, SFTPHost, SFTPPort)
    session.setPassword(SFTPPass)
    val config = new Properties()
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session.connect
    System.out.println("Host connected.")
    val channel = session.openChannel("sftp");
    channel.connect
    val sftpChannel = channel.asInstanceOf[ChannelSftp]

    /*Periodo Anterior de procesamiento*/
    val sustFormatDate = new SimpleDateFormat("yyyyMM")
    val fecha = Calendar.getInstance().getTime
    fecha.setMonth(fecha.getMonth - 1)
    val periodoAnt = sustFormatDate.format(fecha) // val periodoAnt = "202006"

    var lista = new ArrayList[(String, String, String, String)]

    if (recursividad == "S") {
      val sftpLsRoot = sftpChannel.ls(pathRoot + "/*")
      lista = recursividadSFTP(sftpLsRoot, pathRoot, periodoAnt, sftpChannel, arrayExtensions)
    } else {
      lista = SFTPFile(pathRoot, sftpChannel, arrayExtensions)
    }

    val SeqArchivosSftp = lista.toSeq

    /* Inicio de spark , creacion de df */

    val dfFilenames = spark.sparkContext.parallelize(SeqArchivosSftp).toDF("filename", "ruta", "fecmodificacion", "desrutacfg")

    if (dfFilenames.count() > 0) {
      println(s"Creacion del DF  dfFilenames como tempView ruta ")
      dfFilenames.createOrReplaceTempView("tmpListFilenames")
      //cargaSustentoDF.show(10, false)
      //val esquemaDestino = s"${env}_cfiscal_wip"
      //val tablaDestino = "TestSFT"
      spark.sql(s"DROP TABLE IF EXISTS ${esquemaDestino}.${tablaDestino}")
      spark.sql(
        s"""CREATE  TABLE IF NOT EXISTS ${esquemaDestino}.${tablaDestino}
           |stored as ORC
           |location '/claro/cfiscal/${env}/global/data/${esquemaDestino}/${tablaDestino}/'
           |select t.*, 0 as cantidadarchivos from tmpListFilenames t """.stripMargin
      )
    } else {
      println(s"DF  dfFilenames Vacio de la ruta ::")
      // log.error("Se finaliza la ejecucion con codigo error:21")
      sys.exit(21)
      spark.stop()
      spark.close()
    }
    session.disconnect()
    channel.disconnect()
  }


  def recursividadSFTP(sftpLsRoot: Vector[_], pathRoot: String, periodoAnt: String, sftpChannel: ChannelSftp, arrayExtensions: Array[String]): ArrayList[(String, String, String, String)] = {
    val lista = new ArrayList[(String, String, String, String)]

    for (extension <- arrayExtensions) {
      for (f <- 0 to sftpLsRoot.size - 1) {

        try {
          val sftpFile = sftpLsRoot.get(f)
          val sftpLsEntry = sftpFile.asInstanceOf[ChannelSftp#LsEntry]
          val name = sftpLsEntry.getFilename
          if (sftpLsEntry.getAttrs.isDir) {
            if (!(".".equals(name) || "..".equals(name))) {
              //println("nombre Archivo : " + name)//println("nombre Archivo : " + pathRoot + "/" + name + s"/VENTAS/${periodoAnt}/${periodoAnt}*")
              //println("archivo completo : " + b.getLongname) //println(b.getAttrs.isDir)

              val pathVentasPeriodoAnt = s"${pathRoot}/${name}/VENTAS/${periodoAnt}/${periodoAnt}*"
              //////println("hom1 para sftpLsPeriodoAnt ---> " + pathVentasPeriodoAnt)
              val sftpLsPeriodoAnt = sftpChannel.ls(pathVentasPeriodoAnt)

              for (f <- 0 to sftpLsPeriodoAnt.size - 1 if !sftpLsPeriodoAnt.isEmpty) {
                //println("for2 ---p1 --->  ")

                val sftpFile2 = sftpLsPeriodoAnt.get(f)
                val sftpLsEntry2 = sftpFile2.asInstanceOf[ChannelSftp#LsEntry]
                val name2 = sftpLsEntry2.getFilename
                if (sftpLsEntry2.getAttrs.isDir) {
                  if (!(".".equals(name2) || "..".equals(name2))) {

                    //println("nombre Archivo : " + name2)                  //println("peso " + sftpLsEntry2.getAttrs.getSize)
                    //println("archivo completo : " + sftpLsEntry2.getLongname)                  //println(sftpLsEntry2.getAttrs.isDir)
                    //println( name.split("\\.").last)

                    val pathAnioMesDia = pathVentasPeriodoAnt.replace(s"${periodoAnt}*", s"${name2}")
                    /*  println("archivo pathVentasPeriodoAnt : " + pathVentasPeriodoAnt)
                      println("archivo pathAnioMesDia : " + pathAnioMesDia)
                      println("nombre get permission 2  : " + sftpLsEntry2.getAttrs.getPermissionsString)
                                         */
                    val pathAnioMesDiaExtension = pathAnioMesDia + s"/*.${extension}"
                    //  println("nombre get pathAnioMesDiaExtension 2  : " + pathAnioMesDiaExtension)

                    val sftpLsSustentos = sftpChannel.ls(pathAnioMesDiaExtension)
                    //println(" sftpLsSustentos   : " + sftpLsSustentos)
                    //println("nombre get cantidad 2  de sftpLsSustentos   : " + !sftpLsSustentos.isEmpty)

                    for (f <- 0 to sftpLsSustentos.size - 1 if !sftpLsSustentos.isEmpty) {
                      //println("for3 ---p1 --->  ")
                      /*Fecha procesamiento archivos de sustentos*/
                      val formatDate = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
                      val fecProcesamiento = Calendar.getInstance().getTime
                      var fecproc = formatDate.format(fecProcesamiento)

                      val sftpFile3 = sftpLsSustentos.get(f)
                      val sftpLsEntry3 = sftpFile3.asInstanceOf[ChannelSftp#LsEntry]
                      val name3 = sftpLsEntry3.getFilename
                      if (!(sftpLsEntry3.getAttrs.isDir)) {
                        if (!(".".equals(name3) || "..".equals(name3))) {
                          // println("nombre Archivo3 : " + name3)
                          //  println(name3.split("\\.").last)
                          lista.add(name3, pathAnioMesDia, fecproc, pathRoot)
                        }
                      }
                    }
                  }

                }
              }

            }
          }
        }
        catch {
          case e: SftpException => {
            println(s"Error ${e.getCause}")
          }
        }
      }
    }
    lista
  }


  def moveRenameFile(source: String, destination: String): Unit = {
    val sourceFile = ScalaFile(source)
    val targetDir = ScalaFile(destination).createDirectoryIfNotExists()
    println("Mover archivo a ruta backup")
    sourceFile.moveToDirectory(targetDir)
  }

  def SFTPFile(pathRoot: String, sftpChannel: ChannelSftp, arrayExtensions: Array[String]): ArrayList[(String, String, String, String)] = {
    val lista = new ArrayList[(String, String, String, String)]

    for (extension <- arrayExtensions) {

      val sftpLsRoot = sftpChannel.ls(pathRoot + s"/*.${extension}")
      for (f <- 0 to sftpLsRoot.size - 1 if !sftpLsRoot.isEmpty) {
        try {
          val formatDate = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
          val fecProcesamiento = Calendar.getInstance().getTime
          val fecproc = formatDate.format(fecProcesamiento)

          val sftpFile = sftpLsRoot.get(f)
          val sftpLsEntry = sftpFile.asInstanceOf[ChannelSftp#LsEntry]
          val name = sftpLsEntry.getFilename
          if (!(sftpLsEntry.getAttrs.isDir)) {
            if (!(".".equals(name) || "..".equals(name))) {
              // println("nombre Archivo3 : " + name3)
              //  println(name3.split("\\.").last)
              lista.add(name, pathRoot, fecproc, pathRoot)
            }
          }
        }
        catch {
          case e: SftpException => {
            println(s"Error ${e.getCause}")
          }
        }
      }
    }
    lista
  }


  def countFilesFromCompressedFile(rutaCompressedFiles: String, nombreArchivo: String, patronArchivo: String, extension: String): Integer = {
    val rutaCompleta = rutaCompressedFiles + "/" + nombreArchivo
    println("rutaCompleta " + rutaCompleta)
    val inputStream: InputStream = new FileInputStream(rutaCompleta)
    val uncompressedInputStream: CompressorInputStream =
      new CompressorStreamFactory().createCompressorInputStream(
        if (inputStream.markSupported())
          inputStream
        else
          new BufferedInputStream(inputStream)
      )
    val archiveInputStream: ArchiveInputStream =
      new ArchiveStreamFactory().createArchiveInputStream(
        if (uncompressedInputStream.markSupported())
          uncompressedInputStream
        else
          new BufferedInputStream(uncompressedInputStream)
      )
    var ze = archiveInputStream.getNextEntry()
    var contador = 0
    while (ze != null) {
      val fileName = ze.getName()
      ze = archiveInputStream.getNextEntry()
      if (patronArchivo.nonEmpty && fileName.matches(patronArchivo)) {
        contador += 1
      }
    }
    contador
  }

  def validIfExistFile(dfListFiles: DataFrame): ArrayList[(String,String,String)] = {
    val listFileExist = new ArrayList[(String,String,String)]
    val sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    var cont = 0
    //dfListFiles.collect().map(archivo => {
    dfListFiles.rdd.collect().foreach(archivo => {
      val archivoTemp =  new File(archivo.get(0).toString)
      cont += 1
      if (cont%100 == 0) {
        println("numero: " + cont)
      }
      if (archivoTemp.exists()) {
        listFileExist.add(archivoTemp.getName,archivoTemp.getParentFile.toString, sdf.format(archivoTemp.lastModified()))
      }
    }
    )
    listFileExist
  }
}

