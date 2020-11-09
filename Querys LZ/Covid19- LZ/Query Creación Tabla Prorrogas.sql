
CREATE EXTERNAL TABLE PROCESO_BANA_VBEYG.tabla_prorrogas_tmp (
               NO_UNICO BIGINT,
               MONTO FLOAT,
               NO_REFERENCIA DOUBLE, 
               DIAS_PRORROGA STRING,
               REGISTRO_SISTEMAS STRING
) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,' -- Especificar separador de columnas
LOCATION '/user/ralvaren/COVID19' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
TBLPROPERTIES ("skip.header.line.count"="1"); -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.

DROP TABLE PROCESO_BANA_VBEYG.tabla_prorrogas PURGE; -- Eliminar tabla final si ya existe

CREATE TABLE PROCESO_BANA_VBEYG.tabla_prorrogas STORED AS PARQUET AS 
SELECT * FROM PROCESO_BANA_VBEYG.tabla_prorrogas_tmp; -- Crear tabla almacenada como parquet a partir de los 
--datos almacenados en la tabla externa, se debe hacer así ya que por defecto la tabla externa no queda almacenada como parquet.

COMPUTE STATS PROCESO_BANA_VBEYG.tabla_prorrogas;

DROP TABLE PROCESO_BANA_VBEYG.tabla_prorrogas_tmp PURGE; -- Eliminar tabla temporal