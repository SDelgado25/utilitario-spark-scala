package pe.com.claro.util

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object UtilCadenas {

  def contarOcurrenciaEnString(str: String, sub: String): Int = {
    val cnt= str.sliding(sub.length).count(_ == sub)
    cnt
  }

  def obtenerGC1Regex(str:String, reg:String):String= {
    val pattern = new Regex(reg)
    val converted = pattern.replaceAllIn(str, m => m.group(1))
    converted
  }

  def obtenerGC2Regex(str:String, reg:String):String= {
    val pattern = new Regex(reg)
    val converted = pattern.replaceAllIn(str, m => m.group(1)+'|'+m.group(2))
    converted
  }

  def obtenerGC3Regex(str:String, reg:String):String= {
    val pattern = new Regex(reg)
    val converted = pattern.replaceAllIn(str, m =>m.group(1)+'|'+m.group(2)+'|'+m.group(3))
    converted
  }

  def obtenerGC4Regex(str:String, reg:String):String= {
    val pattern = new Regex(reg)
    val converted = pattern.replaceAllIn(str, m =>m.group(1)+'|'+m.group(2)+'|'+m.group(3)+'|'+m.group(4))
    converted
  }

  def obtenerCoincidencia(str:String,reg:String):Array[String]={
    val pattern = new Regex(reg)
    val resultado=pattern.findAllIn(str).toArray
    resultado
  }

  def obtenerGCRegexBkp(str:String, arrayReg:Array[Any]):String= {
    val convertedArray=ArrayBuffer[String]()
    for(reg <- arrayReg){
      val trgCntValReferencia = contarOcurrenciaEnString(reg.toString, "(")
      val convertedValue=if(str.matches(reg.toString)){
        val pattern = new Regex(reg.toString)
        println(s"${str} coincide con el patron ${reg}")
        val converted= trgCntValReferencia match {
          case 1 => pattern.replaceAllIn(str, m => (m.group(1) ))
          case 2 => pattern.replaceAllIn(str, m => m.group(1)+'|'+m.group(2))
          case 3 => pattern.replaceAllIn(str, m => m.group(1)+'|'+m.group(2)+'|'+m.group(3))
          case 4 => pattern.replaceAllIn(str, m => m.group(1)+'|'+m.group(2)+'|'+m.group(3)+'|'+m.group(4))
        }
        converted
      }
      else "NULL"
      println(s"Valor convertido:${convertedValue}")

      if(convertedValue.isEmpty==false)
        convertedArray+=convertedValue.toString
    }
    if(convertedArray.size>1){
      println("ALERTA: El archivo coincide para mas de un parametro")
    }
    val output=convertedArray.mkString(",")
    output
  }

  def obtenerGCRegex(str:String, patron:String):String= {
    val patArray=patron.split("~")
    println(s"Array de patrones:${patArray.foreach(f=>println(f))}")
    val convertedArray=ArrayBuffer[String]()
    for(reg <- patArray){
      println(s"Regex a evaluar:${reg}")
      val trgCntValReferencia = contarOcurrenciaEnString(reg.toString, "(")
      val convertedValue=if(str.matches(reg.toString)){
        val pattern = new Regex(reg.toString)
        var converted= trgCntValReferencia match {
          case 1 => pattern.replaceAllIn(str, m => (m.group(1) ))
          case 2 => pattern.replaceAllIn(str, m => m.group(1)+'|'+m.group(2))
          case 3 => pattern.replaceAllIn(str, m => m.group(1)+'|'+m.group(2)+'|'+m.group(3))
          case 4 => pattern.replaceAllIn(str, m => m.group(1)+'|'+m.group(2)+'|'+m.group(3)+'|'+m.group(4))
          case 5 => pattern.replaceAllIn(str, m => m.group(1)+'|'+m.group(2)+'|'+m.group(3)+'|'+m.group(4)+'|'+m.group(5))
        }
        converted
      } else {
        var convertednulo = trgCntValReferencia match {
          case 1 => "null"
          case 2 => "null|null"
          case 3 => "null|null|null"
          case 4 => "null|null|null|null"
          case 5 => "null|null|null|null|null" //BVA Esto debe estar alineado con la funcion obtenerDFMultiarchivo
        }
        convertednulo
      }
      println(s"Valor convertido:${convertedValue}")
      if(convertedValue.isEmpty==false)
        convertedArray+=convertedValue.toString
    }
    println(convertedArray)
    if(convertedArray.size>1 ){
      println("ALERTA: El archivo coincide para mas de un parametro")
    }
    val output=convertedArray.mkString(",")
    output
  }


}
