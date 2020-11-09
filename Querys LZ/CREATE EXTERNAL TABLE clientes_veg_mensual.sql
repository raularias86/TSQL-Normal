
DROP TABLE IF EXISTS proceso_bana_vbeyg.clientes_veg_mensual_tmp;

CREATE EXTERNAL TABLE proceso_bana_vbeyg.clientes_veg_mensual_tmp (
    fecha_catalogo	TIMESTAMP	,
    no_unico	BIGINT	,
    cliente	STRING	,
    cod_area_financiera	BIGINT	,
    area_financiera	STRING	,
    cod_ejecutivo	INT	,
    nombre_ejecutivo	STRING	,
    tipo_cliente	STRING	,
    no_master	BIGINT	,
    grupo_economico	STRING	,
    tipo_grupo	STRING	,
    nit	STRING	,
    tipo_cartera	STRING	,
    cifcodactivid	STRING	,
    desc_actividad_ciiu	STRING	,
    flag_cde	STRING	,
    flag_relacion	INT	,
    nombre_pais	STRING	,
    sector_idg	STRING	,
    cod_sector_ba	STRING	,
    sector_ba	STRING	,
    cod_subsector_ba	STRING	,
    sub_sector_ba	STRING	,
    cod_actividad_ba	STRING	,
    actividad_ba	STRING	,
    cod_cluster_ba	STRING	,
    cluster_ba	STRING	,
    cod_sectorial_ba	STRING	,
    nivel_riesgo STRING,
    escenario_recuperacion STRING

) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|' -- Especificar separador de columnas
LOCATION '/user/ralvaren/Base_Clientes' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
TBLPROPERTIES ("skip.header.line.count"="1")
; -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.

DROP TABLE IF EXISTS proceso_bana_vbeyg.clientes_veg_mensual PURGE; -- Eliminar tabla final si ya existe

CREATE TABLE proceso_bana_vbeyg.clientes_veg_mensual STORED AS PARQUET AS 
SELECT * FROM proceso_bana_vbeyg.clientes_veg_mensual_tmp; 
-- Crear tabla almacenada como parquet a partir de los datos almacenados en la tabla externa, 
--se debe hacer así ya que por defecto la tabla externa no queda almacenada como parquet.

COMPUTE stats proceso_bana_vbeyg.clientes_veg_mensual;

DROP TABLE IF EXISTS proceso_bana_vbeyg.clientes_veg_mensual_tmp PURGE;