CREATE EXTERNAL TABLE proceso_bana_vbeyg.catalogo_destinos_bcrssf_tmp (
COD_AGRUPADOR_DESTINO	 Int,
DESC_AGRUPADOR_DESTINO	 String,
COD_DESTINO_BCR	 String,
DESC_DESTINO_BCR	 String,
COD_GRUPO_RIESGO	 Int

) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|' -- Especificar separador de columnas
LOCATION '/user/ralvaren/Base_SSF_Clientes/Catalogo_Destinos_BCRSSF' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
TBLPROPERTIES ("skip.header.line.count"="1")
; -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.

DROP TABLE IF EXISTS proceso_bana_vbeyg.catalogo_destinos_bcrssf PURGE; -- Eliminar tabla final si ya existe

CREATE TABLE proceso_bana_vbeyg.catalogo_destinos_bcrssf STORED AS PARQUET AS 
SELECT * FROM proceso_bana_vbeyg.catalogo_destinos_bcrssf_tmp; 
-- Crear tabla almacenada como parquet a partir de los datos almacenados en la tabla externa, 
--se debe hacer así ya que por defecto la tabla externa no queda almacenada como parquet.

DROP TABLE IF EXISTS proceso_bana_vbeyg.catalogo_destinos_bcrssf_tmp PURGE;

select * from proceso_bana_vbeyg.catalogo_destinos_bcrssf limit 100;
