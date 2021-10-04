################################################################################################################################
# LOG_PROCESS 
################################################################################################################################
#!/bin/bash
IDPROCESS=$1
LOGNAME=$2
LOGPATH=$3

echo "**********seteo de parametros IDPROCESS :  ${IDPROCESS}"

echo "**********Inicio de obtencion del application ID"
hdfs dfs -get /var/logs/${IDPROCESS} .

flg_hdfs_get=$?
if [ $flg_hdfs_get -eq 0 ]
then
	echo "=========>EXITO flg_hdfs_get Obtencion exitosa de la ruta /var/logs/${IDPROCESS}  -----> log_process.sh "
else
	echo "=========>ERROR flg_hdfs_get Obtencion errada de la ruta /var/logs/${IDPROCESS}  -----> log_process.sh "
	exit 1
fi

application_id=$(cat ${IDPROCESS})
echo "${application_id}"

echo "**********FIN de obtencion del application ID"

echo "INICIO source /FusionInsight_Client/Yarn/Yarn/component_env "

##source /dataingesta/YARN/Yarn/component_env   ## servidor.228
##source /desabigdata/FusionInsight_Client/Yarn/Yarn/component_env  ## servidor .173
source /srv/BigData/Huawei/FusionInsight/Yarn/Yarn/component_env  ## dev,qas,prd

echo "FIN DE EJECUTAR el source /FusionInsight_Client/Yarn/Yarn/component_env"

echo "**********INICIO de obtencion del YARN LOG CON application ID  ${application_id}  DEL LOG_FILE  ${LOGNAME}_TRAZADO_TMP.log "
yarn logs -applicationId ${application_id} -log_files ${LOGNAME}_TRAZADO.log> ${LOGNAME}_TRAZADO_TMP.log
flg_yarn_log_traza=$?
if [ $flg_yarn_log_traza -eq 0 ]
then
	echo "=========>EXITO flg_yarn_log_traza creacion del log temporal  -----> log_process.sh "
else
	echo "=========>ERROR flg_yarn_log_traza creacion del log temporal  -----> log_process.sh "
	exit 1
fi


cat ${LOGNAME}_TRAZADO_TMP.log | grep  '^[0-9]' >> ${LOGPATH}${LOGNAME}_TRAZADO.log


flg_cat_log_traza=$?
if [ $flg_cat_log_traza -eq 0 ]
then
	echo "=========>EXITO flg_cat_log_traza escribir log final  ${LOGPATH}${LOGNAME}_TRAZADO.log  -----> log_process.sh "
else
	echo "=========>ERROR flg_cat_log_traza escribir log final  ${LOGPATH}${LOGNAME}_TRAZADO.log  -----> log_process.sh "
	exit 1
fi
echo "**********FIN de obtencion del YARN LOG CON application ID  ${application_id}  DEL LOG_FILE ${LOGNAME}_TRAZADO.log "


echo "**********INICIO de obtencion del YARN LOG CON application ID  ${application_id}  DEL LOG_FILE ${LOGNAME}_TRAZADO.log "
yarn logs -applicationId ${application_id} -log_files ${LOGNAME}_ELK.txt> ${LOGNAME}_ELK_TMP.txt
flg_yarn_log_elk=$?
if [ $flg_yarn_log_elk -eq 0 ]
then
	echo "=========>EXITO flg_yarn_log_elk creacion del log temporal  -----> log_process.sh "
else
	echo "=========>ERROR flg_yarn_log_elk creacion del log temporal  -----> log_process.sh "
	exit 1
fi

### cat ${LOGNAME}-elk-driver-tmp.log | grep  '^[0-9]' >> /var/logs/${LOGNAME}-elk-driver.log
### cat ${LOGNAME}-elk-driver-tmp.log | grep  '^[0-9]' >> /desabigdata/developers/u183207/log_cluster/${LOGNAME}-elk-driver.log

cat ${LOGNAME}_ELK_TMP.txt | grep  '^[0-9]' >> ${LOGPATH}${LOGNAME}_ELK.txt

flg_cat_log_elk=$?
if [ $flg_cat_log_elk -eq 0 ]
then
	echo "=========>EXITO flg_cat_log_elk escribir log final  ${LOGPATH}${LOGNAME}_ELK.txt  -----> log_process.sh "
else
	echo "=========>ERROR flg_cat_log_elk escribir log final  ${LOGPATH}${LOGNAME}_ELK.txt  -----> log_process.sh "
	exit 1
fi

echo "**********FIN de obtencion del YARN LOG CON application ID  ${application_id}  DEL LOG_FILE  ${LOGPATH}${LOGNAME}_ELK.txt "

echo "################# IDPROCESS LOG #################"


echo "**********INICIO de limpieza de archivos temporales "

echo "**********INICIO del hdfs dfs -rm /var/logs/${IDPROCESS} "
hdfs dfs -rm /var/logs/${IDPROCESS}
flg_hdfs_rm=$?
if [ $flg_hdfs_rm -eq 0 ]
then
	echo "=========>EXITO flg_hdfs_rm Eliminacion exitosa de la ruta /var/logs/${IDPROCESS}  -----> log_process.sh "
else
	echo "=========>ERROR flg_hdfs_rm Eliminacion errada de la ruta /var/logs/${IDPROCESS}  -----> log_process.sh "
	exit 1
fi
echo "**********FIN del hdfs dfs -rm /var/logs/${IDPROCESS} "

echo "**********INICIO del rm -r ${LOGNAME}-trazado-driver-tmp.log ${LOGNAME}-elk-driver-tmp.log  "
rm ${LOGNAME}_TRAZADO_TMP.log ${LOGNAME}_ELK_TMP.txt ${IDPROCESS}

flg_rm_r=$?
if [ $flg_rm_r -eq 0 ]
then
	echo "=========>EXITO flg_rm_r Eliminacion exitosa ${LOGNAME}_TRAZADO.log ${LOGNAME}_ELK.txt   -----> log_process.sh "
else
	echo "=========>ERROR flg_rm_r Eliminacion errada ${LOGNAME}_TRAZADO.log ${LOGNAME}_ELK.txt   -----> log_process.sh "
	exit 1
fi
echo "**********FIN del rm -r ${LOGNAME}-trazado-driver-tmp.log ${LOGNAME}-elk-driver-tmp.log  "
 

echo "**********FIN de limpieza de archivos temporales "
 
echo "********** Se proceso el log para el application ID  ${application_id} del IDPROCESS  ${IDPROCESS}"
