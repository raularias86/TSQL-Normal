CREATE EXTERNAL TABLE proceso_bana_vbeyg.tabla_sectores_ssf_tmp (
               fch_corte string,
               anio int,
               mes int,
               tipo_date STRING, 
               sectores_economicos STRING,
               destinos_economicos string,
               institucion STRING,
               saldo double
) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,' -- Especificar separador de columnas
LOCATION '/user/ralvaren/Base_Sectores_SSF' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
TBLPROPERTIES ("skip.header.line.count"="1")
; -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.

DROP TABLE IF EXISTS proceso_bana_vbeyg.tabla_sectores_ssf_tmp PURGE; -- Eliminar tabla final si ya existe

CREATE TABLE proceso_bana_vbeyg.tabla_sectores_ssf STORED AS PARQUET AS 
SELECT * FROM proceso_bana_vbeyg.tabla_sectores_ssf_tmp; 
-- Crear tabla almacenada como parquet a partir de los datos almacenados en la tabla externa, 
--se debe hacer así ya que por defecto la tabla externa no queda almacenada como parquet.

DROP TABLE proceso_bana_vbeyg.tabla_sectores_ssf_tmp PURGE; -- Eliminar tabla temporal


SELECT * FROM proceso_bana_vbeyg.tabla_sectores_ssf
; 
