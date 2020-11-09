
CREATE EXTERNAL TABLE PROCESO_BANA_VBEYG.catalogo_sectorial_tmp (
        Fch_Catalogo	 TIMESTAMP,
        COD_PORTAL	 string,
        DESC_PORTAL	 string,
        FLAG_SISTEMAS_BA	 string,
        COD_SECTOR_BA	 string,
        SECTOR_BA	 string,
        COD_SUBSECTOR_BA	 string,
        SUB_SECTOR_BA	 string,
        COD_ACTIVIDAD_BA	 string,
        ACTIVIDAD_BA	 string,
        COD_CLUSTER_BA	 string,
        CLUSTER_BA	 string,
        COD_SECTORIAL_BA	 string,
        COD_SECTOR_CORPORATIVO	 string,
        SECTOR_CORPORATIVO	 string,
        COD_MACROSECTOR_CIIU	 string,
        MACROSECTOR_CIIU	 string,
        NIVEL_RIESGO	 string,
        ESCENARIO_RECUPERACION  string
) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|' -- Especificar separador de columnas
LOCATION '/user/ralvaren/Catalogos_Sectoriales' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
TBLPROPERTIES ("skip.header.line.count"="1"); -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.

DROP TABLE IF EXISTS PROCESO_BANA_VBEYG.catalogo_sectorial PURGE; -- Eliminar tabla final si ya existe

CREATE TABLE PROCESO_BANA_VBEYG.catalogo_sectorial STORED AS PARQUET AS 
SELECT * FROM PROCESO_BANA_VBEYG.catalogo_sectorial_tmp; -- Crear tabla almacenada como parquet a partir de los 
--datos almacenados en la tabla externa, se debe hacer así ya que por defecto la tabla externa no queda almacenada como parquet.

COMPUTE STATS PROCESO_BANA_VBEYG.catalogo_sectorial;

DROP TABLE PROCESO_BANA_VBEYG.catalogo_sectorial_tmp PURGE; -- Eliminar tabla temporal


select * 
from PROCESO_BANA_VBEYG.catalogo_sectorial
;

---AL TENERLO CARGADO EL CATÁLOGO SECTORIAL HACER EL PROCESO PARA CARGARLO EN resultados_bana_vbeyg.catalogo_sectorial_ba
--1. extraer la estructura de la tabla de proceso_bana_vbeyg.catalogo_sectorial:
show create table proceso_bana_vbeyg.catalogo_sectorial;

--2. modificar la estructura para resultados bana, agregando las columnas de ingestión y definiendo la partición:
CREATE TABLE resultados_bana_vbeyg.catalogo_sectorial_ba ( 
    ingestion_year INT,   
    ingestion_month INT,
    ingestion_day INT,   
    fch_catalogo TIMESTAMP,   
    cod_portal STRING,   
    desc_portal STRING, 
    flag_sistemas_ba STRING,
    cod_sector_ba STRING,   
    sector_ba STRING,   
    cod_subsector_ba STRING,   
    sub_sector_ba STRING,   
    cod_actividad_ba STRING,   
    actividad_ba STRING,   
    cod_cluster_ba STRING,   
    cluster_ba STRING,   
    cod_sectorial_ba STRING,   
    cod_sector_corporativo STRING,   
    sector_corporativo STRING,   
    cod_macrosector_ciiu STRING,  
     macrosector_ciiu STRING,   
     nivel_riesgo STRING
     ) 
partitioned  BY (
    year int 
)
STORED AS PARQUET ;

--3. agregar la partición al año 2020
alter table resultados_bana_vbeyg.catalogo_sectorial_ba 
add partition (year = 2020);

--4. por último, agregar los datos ya en la nueva tabla, a la partición respectiva: 
insert into resultados_bana_vbeyg.catalogo_sectorial_ba 
partition (year = 2020)
select year(now()) ingestion_year,
       month(now()) ingestion_month, 
       day(now()) ingestion_day,
       fch_catalogo,
       cod_portal, 
       desc_portal,
       flag_sistemas_ba,
       cod_sector_ba,
       sector_ba,
       cod_subsector_ba,
       sub_sector_ba,
       cod_actividad_ba,
       actividad_ba,
       cod_cluster_ba,
       cluster_ba, 
       cod_sectorial_ba,
       cod_sector_corporativo,
       sector_corporativo,
       cod_macrosector_ciiu, 
       macrosector_ciiu,
       nivel_riesgo,
       escenario_recuperacion
from proceso_bana_vbeyg.catalogo_sectorial
;

