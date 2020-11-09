CREATE TABLE resultados_bana_vbeyg.catalogo_sectorial_ba ( 
    ingestion_year INT,   
    ingestion_month INT,
    ingestion_day INT,   
    fch_catalogo TIMESTAMP,   
    cod_portal STRING,   
    desc_portal STRING,   
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

alter table resultados_bana_vbeyg.catalogo_sectorial_ba 
add partition (year = 2020);

----------------- para hacer el insert ------------------------

insert into resultados_bana_vbeyg.catalogo_sectorial_ba 
partition (year = 2020)
select year(now()) ingestion_year,
       month(now()) ingestion_month, 
       day(now()) ingestion_day,
       fch_catalogo,
       cod_portal, 
       desc_portal,
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