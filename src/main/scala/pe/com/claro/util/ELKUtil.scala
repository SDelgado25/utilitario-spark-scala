package pe.com.claro.util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger
// import org.apache.logging.log4j.{LogManager, ThreadContext}

object ELKUtil {

  val logger: Logger = Logger.getLogger(this.getClass)
  // val logger = LogManager.getLogger(this.getClass)

  val SEPARADOR_ELK=GlobalConstants.SEPARADOR_ELK

  val COD_CAPA=GlobalConstants.COD_CAPA_RDV
  val TIPO_PROCESO=GlobalConstants.TIPO_PROCESO_INGESTA

  def isEmpty(x: String) = x == null || x.trim.isEmpty

  def logInvocacionMetodo(p_rutalog:String,nombreFichero:String,codEjecMalla:String,
                          tipoFuente:Int,nomProceso:String,origen:String,codTarea:String,
                          tipoMensaje:Int, mensaje:String,idTarea:String,
                          IDTransaccion:String, tiempoRespuesta:Long,hostname:String,env:String):Unit={

    // TODO mrivas Activar ThreadContext
    // ThreadContext.put("logFileNameELK", s"${nombreFichero}_ELK")
    // ThreadContext.put("logPath", p_rutalog)
    // ThreadContext.put("SEPARADOR_ELK", SEPARADOR_ELK)

    val now = Calendar.getInstance().getTime()
    val formater = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val nowString = formater.format(now)

    var logMessageELK=new StringBuilder("")
        .append(if(isEmpty(codEjecMalla.toString)) "" else codEjecMalla+SEPARADOR_ELK)
        .append(if(isEmpty(tipoFuente.toString)) "" else tipoFuente.toString).append(SEPARADOR_ELK)
        .append(if(isEmpty(COD_CAPA.toString)) "" else COD_CAPA).append(SEPARADOR_ELK)
        .append(if(isEmpty(TIPO_PROCESO.toString)) "" else TIPO_PROCESO).append(SEPARADOR_ELK)
        .append(if(isEmpty(nomProceso.toString)) "" else nomProceso).append(SEPARADOR_ELK)
        .append(if(isEmpty(origen.toString)) "" else origen).append(SEPARADOR_ELK)
        .append(if(isEmpty(codTarea.toString)) "" else codTarea).append(SEPARADOR_ELK)
        .append(if(isEmpty(tipoMensaje.toString)) "" else tipoMensaje.toString).append(SEPARADOR_ELK)
        .append(if(isEmpty(mensaje.toString)) "" else mensaje).append(SEPARADOR_ELK)
        .append(if(isEmpty(idTarea.toString)) "" else idTarea+"_"+nowString).append(SEPARADOR_ELK) //Correlativo
        .append(if(isEmpty(IDTransaccion.toString)) "" else IDTransaccion).append(SEPARADOR_ELK) //IDTransaccion
        .append(if(isEmpty(tiempoRespuesta.toString)) "0" else tiempoRespuesta.toString).append(SEPARADOR_ELK)
        .append(if(isEmpty(hostname.toString)) "" else hostname).append(SEPARADOR_ELK)
        .append(if(isEmpty(env.toString)) "" else env)

    logger.info(logMessageELK)

    // TODO mrivas Activar ThreadContext
    // ThreadContext.remove("logFileNameELK")
  }

}
