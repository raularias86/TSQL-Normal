CREATE EXTERNAL TABLE proceso_bana_vbeyg.tabla_productos_tmp (
               FCH_PROCESO TIMESTAMP,
               COD_AREA_FINANCIERA BIGINT,
               COD_ASIGNADO BIGINT,
               NO_UNICO BIGINT, 
               AGRUPADOR STRING,
               COD_PRODUCTO BIGINT,
               NOMBRE_PRODUCTO STRING,
               NO_CUENTA BIGINT
) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,' -- Especificar separador de columnas
LOCATION '/user/ralvaren/Base_Otros_Productos' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
--TBLPROPERTIES ("skip.header.line.count"="1")
; -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.

DROP TABLE IF EXISTS proceso_bana_vbeyg.tabla_otros_productos PURGE; -- Eliminar tabla final si ya existe
CREATE TABLE proceso_bana_vbeyg.tabla_externa STORED AS PARQUET AS 
SELECT * FROM proceso_bana_vbeyg.tabla_productos_tmp; 
-- Crear tabla almacenada como parquet a partir de los datos almacenados en la tabla externa, 
--se debe hacer así ya que por defecto la tabla externa no queda almacenada como parquet.

DROP TABLE IF EXISTS proceso_bana_vbeyg.tabla_externa_tmp PURGE; -- Eliminar tabla temporal
