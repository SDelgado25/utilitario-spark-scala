package pe.com.claro.util;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.spark.sql.SparkSession;

import java.io.File;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class Log4jUtil {
    private String codEjecucion;
    private String tipoFuente;
    private String codCapa;
    private String tipoProceso;
    private String nombreProceso;
    private String codTarea;
    private String codRespuesta;
    private String idTransaccion;
    private String hostname;
    private String entorno;
    private Class object;
    private String tiempoProceso;
    private String idAuditoria;
    private static SparkSession spark;


    static {
        // DOMConfigurator.configure("/opt/claro/nifi_desa/log4j.properties");
        System.out.println("Metodo estatico");
        spark = SparkSession.builder().getOrCreate();
        // System.out.println(session.sparkContext().applicationId());
/*
        System.out.println(System.getenv("SPARK_YARN_STAGING_DIR"));
        System.out.println("Variables de Entorno");
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            System.out.println("Key [" + entry.getKey() + "] Value [" + entry.getValue() + "]");
        }

        System.out.println("");
        System.out.println("Propiedades");
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            System.out.println("Key [" + entry.getKey() + "] Value [" + entry.getValue() + "]");
        }

        // DOMConfigurator.configure(System.getenv("SPARK_YARN_STAGING_DIR") + "/log4j.properties");
 */
    }

    Logger logger = Logger.getLogger(Log4jUtil.class);

    public Log4jUtil() {

    }

    public Log4jUtil(String codEjecucion, String tipoFuente, String codCapa, String tipoProceso, String nombreProceso, String codTarea, String codRespuesta, String idTransaccion, String hostname, String entorno, Class object, String tiempoProceso, String idAuditoria) {
        this.codEjecucion = codEjecucion;
        this.tipoFuente = tipoFuente;
        this.codCapa = codCapa;
        this.tipoProceso = tipoProceso;
        this.nombreProceso = nombreProceso;
        this.codTarea = codTarea;
        this.codRespuesta = codRespuesta;
        this.idTransaccion = idTransaccion;
        this.hostname = hostname;
        this.entorno = entorno;
        this.object = object;
        this.tiempoProceso = tiempoProceso;
        this.idAuditoria = idAuditoria;

        MDC.clear();
        MDC.put("codEjecucion", codEjecucion);
        MDC.put("tipoFuente", tipoFuente);
        MDC.put("codCapa", codCapa);
        MDC.put("tipoProceso", tipoProceso);
        MDC.put("nombreProceso", nombreProceso);
        MDC.put("codTarea", codTarea);
        MDC.put("codRespuesta", codRespuesta);
        MDC.put("idTransaccion", idTransaccion);
        MDC.put("hostname", hostname);
        MDC.put("entorno", entorno);
    }

    public void printPerformanceMessage(String hostname, Long duracion, String codigo, String mensaje) {
        logger.info(generatePerformanceMessage(hostname, duracion, codigo, mensaje));
    }


    private String generatePerformanceMessage(String hostname, Long duracion, String codigo, String mensaje) {
        StringBuilder sb = new StringBuilder();

        sb.append(idAuditoria)
                .append("|")
                .append(hostname)
                .append("|")
                .append(duracion)
                .append("|")
                .append(codigo)
                .append("|")
                .append(mensaje);

        return sb.toString();
    }

    public String generateTrazadoMessage(String mensaje) {
        StringBuilder sb = new StringBuilder();

        sb.append(hostname)
                .append("|")
                .append(mensaje);
        return sb.toString();
    }

    public String getCodEjecucion() {
        return codEjecucion;
    }

    public void setCodEjecucion(String codEjecucion) {
        this.codEjecucion = codEjecucion;
        MDC.put("codEjecucion", codEjecucion);

    }

    public String getTipoFuente() {
        return tipoFuente;
    }

    public void setTipoFuente(String tipoFuente) {
        this.tipoFuente = tipoFuente;
        MDC.put("tipoFuente", tipoFuente);

    }

    public String getCodCapa() {
        return codCapa;
    }

    public void setCodCapa(String codCapa) {
        this.codCapa = codCapa;
        MDC.put("codCapa", codCapa);

    }

    public String getTipoProceso() {
        return tipoProceso;
    }

    public void setTipoProceso(String tipoProceso) {
        this.tipoProceso = tipoProceso;
        MDC.put("tipoProceso", tipoProceso);

    }

    public String getNombreProceso() {
        return nombreProceso;
    }

    public void setNombreProceso(String nombreProceso) {
        this.nombreProceso = nombreProceso;
        MDC.put("nombreProceso", nombreProceso);

    }

    public String getCodTarea() {
        return codTarea;
    }

    public void setCodTarea(String codTarea) {
        this.codTarea = codTarea;
        MDC.put("codTarea", codTarea);

    }

    public String getCodRespuesta() {
        return codRespuesta;
    }

    public void setCodRespuesta(String codRespuesta) {
        this.codRespuesta = codRespuesta;
        MDC.put("codRespuesta", codRespuesta);

    }

    public String getIdTransaccion() {
        return idTransaccion;
    }

    public void setIdTransaccion(String idTransaccion) {
        this.idTransaccion = idTransaccion;
        MDC.put("idTransaccion", idTransaccion);

    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
        MDC.put("hostname", hostname);

    }


    public String getEntorno() {
        return entorno;
    }

    public void setEntorno(String entorno) {
        if (entorno.equals("dev")) {
            this.entorno = "1";
        } else if (entorno.equals("qas")) {
            this.entorno = "2";
        } else if (entorno.equals("prd")) {
            this.entorno = "3";
        }
        MDC.put("entorno", this.entorno);

    }

    public Class getObject() {
        return object;
    }

    public void setObject(Class object) {
        this.object = object;
    }

    public String getTiempoProceso() {
        return tiempoProceso;
    }

    public void setTiempoProceso(String tiempoProceso) {
        this.tiempoProceso = tiempoProceso;
    }

    public String getIdAuditoria() {
        return idAuditoria;
    }

    public void setIdAuditoria(String idAuditoria) {
        this.idAuditoria = idAuditoria;
    }

    public void processLog(String applicationId) {
        System.out.println("Process Log [" + applicationId + "]");

        try {
            ProcessBuilder pb = new ProcessBuilder("process_log.sh ", applicationId /* , "myArg2" */);
            pb.directory(new File("hdfs:///hacluster/var/logs"));
            // pb.directory(new File("file:///desabigdata/developers/u185424"));

            System.out.println(pb.directory().getAbsolutePath());
            Process p = pb.start();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
        }

    }

}
