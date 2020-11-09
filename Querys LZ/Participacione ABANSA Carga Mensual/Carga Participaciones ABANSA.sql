
CREATE EXTERNAL TABLE PROCESO_BANA_VBEYG.participaciones_abansa_tmp (
        fecha	 STRING,
        indicador	 string,
        institucion	 string,
        saldo	         double
) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;' -- Especificar separador de columnas
LOCATION '/user/ralvaren/Bases_Participaciones_Abansa' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
TBLPROPERTIES ("skip.header.line.count"="1"); -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.

DROP TABLE PROCESO_BANA_VBEYG.participaciones_abansa PURGE; -- Eliminar tabla final si ya existe

CREATE TABLE PROCESO_BANA_VBEYG.participaciones_abansa STORED AS PARQUET AS 
SELECT fecha, indicador, institucion, saldo, 
lag(saldo) over(partition by indicador, institucion order by fecha) saldo_mes_anterior 
FROM PROCESO_BANA_VBEYG.participaciones_abansa_tmp; -- Crear tabla almacenada como parquet a partir de los 
--datos almacenados en la tabla externa, se debe hacer así ya que por defecto la tabla externa no queda almacenada como parquet.

COMPUTE stats PROCESO_BANA_VBEYG.participaciones_abansa;

DROP TABLE PROCESO_BANA_VBEYG.participaciones_abansa_tmp PURGE; -- Eliminar tabla temporal

select * 
from PROCESO_BANA_VBEYG.participaciones_abansa
limit 100
;