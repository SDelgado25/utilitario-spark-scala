package pe.com.claro.util

/**
  * Clase para contener las constantes globales utilzadas en los demás proyectos
  *
  */
object GlobalConstants {
  //Constantes Globales
  val COUNTRY = "PE"
  val LANGUAGE = "ES"
  val LOCAL_CURRENCY = "PEN"
  val DATE_SEPARATOR = "-"

  val PROCESS_DATA_TYPE_DAILY = "diaria"
  val PROCESS_DATA_TYPE_HISTORICAL = "historica"

  val MODE_OVERWRITE = "overwrite"
  val MODE_APPEND = "append"
  val MODE_REPROCESS = "reprocess"

  val COMPLETE_LOAD = "completa"
  val INCREMENTAL_LOAD = "incremental"

  //Constantes Conexion
  val DEFAULT_PATH_CFG_CONEX_FS="/claro/cfiscal/env/global/data/env_cfiscal_cfg/origen_conex_fs/"
  val DEFAULT_PATH_CFG_CONEX_WS="/claro/cfiscal/env/global/data/env_cfiscal_cfg/origen_conex_ws/"
  val DEFAULT_PATH_CFG_PARAM_CONEX_WS="/claro/cfiscal/env/global/data/env_cfiscal_cfg/param_conex_ws/"

  //Constantes: Procesos de Ingesta Tipo estructurado
  val DEFAULT_PATH_SRC_BD_DEF="/claro/cfiscal/env/global/data/env_cfiscal_cfg/estruct_bd_origen/"
  val DEFAULT_PATH_SRC_FILE_DEF="/claro/cfiscal/env/global/data/env_cfiscal_cfg/estruct_file_origen/"
  val DEFAULT_PATH_TRG_DEF="/claro/cfiscal/env/global/data/env_cfiscal_cfg/estruct_destino/"
  val DEFAULT_PATH_MALLA_DEF="/claro/cfiscal/env/global/data/env_cfiscal_cfg/malla_wf_ingesta/"
  val DEFAULT_PATH_PARAMS_BD_FILTRO="/claro/cfiscal/env/global/data/env_cfiscal_cfg/params_bd_filtro/"
  val DEFAULT_PATH_TRAZAB_EJEC="/claro/cfiscal/env/global/data/env_cfiscal_ctrl/ctrl_trazab_ingesta_estruct"
  val DEFAULT_PATH_CTRL_EJEC="/claro/cfiscal/env/global/data/env_cfiscal_ctrl/ctrl_ejecuc_ingesta_estruct"

  //Constantes: Procesos de Ingesta Tipo no estructurado
  val DEFAULT_PATH_SRC_NOESTRUCT="/claro/cfiscal/env/global/data/env_cfiscal_cfg/soportes_origen/"
  val DEFAULT_PATH_PARAMS_NOESTRUCT="/claro/cfiscal/env/global/data/env_cfiscal_cfg/param_soportes/"
  val DEFAULT_PATH_CTRL_REG_SOPORTE="/claro/cfiscal/env/global/data/env_cfiscal_ctrl/ctrl_registro_soporte/"
  val DEFAULT_PATH_CFG_MALLA_SOPORTE="/claro/cfiscal/env/global/data/env_cfiscal_cfg/malla_wf_soportes/"
  val DEFAULT_PATH_CTRL_REG_SOPORTES_GEN="/claro/cfiscal/env/global/data/env_cfiscal_ctrl/ctrl_registro_soporte_general/"
  val DEFAULT_PATH_COMPROBANTES_CTA19="/claro/cfiscal/env/global/data/env_cfiscal_ctrl/pcd_cta19_comprobantes_ingesta/"
  val DEFAULT_PATH_TEMP="/claro/cfiscal/env/global/temp/"
  val FLG_ACTIVO="S"
  val LEVEL_PARALLELISM="16"

  //Constantes: Log ELK Procesos de Ingesta
  val SEPARADOR_ELK="|"
  val TIPO_FUENTE_ESTRUCTURADO_ARCHIVO=1
  val TIPO_FUENTE_NOESTRUCTURADO_ARCHIVO=2
  val TIPO_FUENTE_ESTRUCTURADO_BD=3
  val COD_CAPA_RDV=1
  val COD_CAPA_CDV=2
  val COD_CAPA_BDV=3
  val TIPO_PROCESO_INGESTA=1
  val TIPO_PROCESO_PROCESAMIENTO=2
  val TIPO_MENSAJE_OK=1
  val TIPO_MENSAJE_ERROR=2

  //Constantes tamaño de bloque de acuerdo a prioridad
  val BLOQUE_PRIORIDAD_MUY_ALTA="8000000"
  val BLOQUE_PRIORIDAD_ALTA="4000000"
  val BLOQUE_PRIORIDAD_MEDIA="2000000"
  val BLOQUE_PRIORIDAD_BAJA="2000000"

  val ESQUEMA_CTRL=s"_cfiscal_ctrl"


}
