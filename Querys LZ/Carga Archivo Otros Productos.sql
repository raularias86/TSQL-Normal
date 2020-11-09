CREATE EXTERNAL TABLE proceso_bana_vbeyg.vta_cruzado_oprod_tmp (
               fch_proceso string,
               cod_area_financiera int,
               cod_asignado int,
               no_unico bigint,
               agrupador_producto string,
               cod_producto int, 
               nombre_producto string, 
               no_referencia BIGINT
) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' -- Especificar separador de columnas
LOCATION '/user/ralvaren/Base_Otros_Productos' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
--TBLPROPERTIES ("skip.header.line.count"="1") -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.
; 

DROP TABLE IF EXISTS proceso_bana_vbeyg.vta_cruzado_otrosprod PURGE; -- Eliminar tabla final si ya existe

CREATE TABLE proceso_bana_vbeyg.vta_cruzado_otrosprod STORED AS PARQUET AS 
SELECT * FROM proceso_bana_vbeyg.vta_cruzado_oprod_tmp; 
-- Crear tabla almacenada como parquet a partir de los datos almacenados en la tabla externa, 
--se debe hacer así ya que por defecto la tabla externa no queda almacenada como parquet.

compute stats proceso_bana_vbeyg.vta_cruzado_otrosprod;

DROP TABLE IF EXISTS proceso_bana_vbeyg.vta_cruzado_oprod_tmp PURGE;