package pe.com.claro.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object UtilSeries {

  /** Obtiene array de series de acuerdo a filtro en legado [ISIN] y utilidad [ISIN].
   *
   *  ==Parametros que recibe==
   *  spark: Sesion Spark.
   *  env: Ambiente de ejecucion.
   *  param1_legado: Lista de valores en campo legado a buscar (isin). Ejemplo Seq("BSCS_7"), Seq("BSCS_7","SGA").
   *  param2_utilidad: Lista de valores en campo utilidad a buscar (isin). Ejemplo Seq("RECIBO"), Seq("RECIBO","NC").
   *  ==Retorna==
   *  Array[String] con valores del campo serie resultantes de los filtros en campos legado y utilidad
   * ==Uso para filtro==
   * filter(col("serie").isin(obtieneSeriesRDV(spark,env,param1_legado,param2_utilidad):_*)
   */
  def obtieneSeriesRDV(spark:SparkSession, env:String,
                       param1_legado:Seq[String],
                       param2_utilidad:Seq[String]
                      ):Array[String]={
    import spark.implicits._
    val nomtabla="pcd_parametro_serie"
    val arraySeries=spark.sql(
      s"select * from ${env}_cfiscal_rdv_reportes.${nomtabla}"
    ).filter(
      col("legado").isin(param1_legado:_*) &&
        col("utilidad").isin(param2_utilidad:_*)
    ).select(
      col("serie")
    ).dropDuplicates().map(_.getString(0)).collect
    arraySeries
  }

  /** Obtiene array de series de acuerdo a filtro en codlegado [ISIN] y desambito [ISIN].
   *
   *  ==Parametros que recibe==
   *  spark: Sesion Spark.
   *  env: Ambiente de ejecucion.
   *  param1_legado: Lista de valores en campo codlegado a buscar (isin). Ejemplo Seq("BSCS_7"), Seq("BSCS_7","SGA").
   *  param2_desambito: Lista de valores en campo desambito a buscar (isin). Ejemplo Seq("RECIBO"), Seq("RECIBO","NC").
   *  ==Retorna==
   *  Array[String] con valores del campo codserie resultantes de los filtros en campos codlegado y desambito
   * ==Uso para filtro==
   * filter(col("serie").isin(obtieneSeriesCDV(spark,env,param1_legado,param2_desambito):_*)
   */
  def obtieneSeriesCDV(spark:SparkSession, env:String,
                       param1_legado:Seq[String],
                       param2_desambito:Seq[String]
                      ):Array[String]={
    import spark.implicits._
    val nomtabla="SERIEFACTURACION"
    val arraySeries=UtilParquet.obtenerDataCDV(
      spark,env,nomtabla,"",true
    ).filter(
      col("codlegado").isin(param1_legado:_*) &&
        col("desambito").isin(param2_desambito:_*)
    ).select(
      col("codserie")
    ).dropDuplicates().map(_.getString(0)).collect
    arraySeries
  }

}
