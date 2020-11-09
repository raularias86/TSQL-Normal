CREATE EXTERNAL TABLE proceso_bana_vbeyg.base_grupos_economicos_tmp (
    NO_UNICO_CLIENTE	 bigint,
    NO_UNICO_GRUPO	 bigint,
    NOMBRE_GRUPO	 string,
    TIPO_GRUPO	 string,
    COD_EJECUTIVO_GRUPO	 bigint
) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|' -- Especificar separador de columnas
LOCATION '/user/ralvaren/Base_SSF_Clientes/Base_Grupos_Economicos' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
TBLPROPERTIES ("skip.header.line.count"="1")
; -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.

DROP TABLE IF EXISTS proceso_bana_vbeyg.tabla_otros_productos PURGE; -- Eliminar tabla final si ya existe

CREATE TABLE proceso_bana_vbeyg.base_grupos_economicos STORED AS PARQUET AS 
SELECT * FROM proceso_bana_vbeyg.base_grupos_economicos_tmp; 
-- Crear tabla almacenada como parquet a partir de los datos almacenados en la tabla externa, 
--se debe hacer así ya que por defecto la tabla externa no queda almacenada como parquet.

DROP TABLE IF EXISTS proceso_bana_vbeyg.base_grupos_economicos_tmp PURGE;

select * from proceso_bana_vbeyg.base_grupos_economicos limit 100;