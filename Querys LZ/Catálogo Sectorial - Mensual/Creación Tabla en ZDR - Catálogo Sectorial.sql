
/****************
PROCESO A SEGUIR UNA VEZ, LA DE CREACIÓN Y DEFINICIÓN EN ÁREA DE RESULTADOS PARA TABLA DE CATÁLOGO SECTORIAL, 
LUEGO DE ESTO SOLO APLICA EL PASO 4, PARA IR INSERTANDO LOS NUEVOS VALORES MENSUALES DEL CATÁLOGO
****************/

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
       nivel_riesgo
from proceso_bana_vbeyg.catalogo_sectorial
;

