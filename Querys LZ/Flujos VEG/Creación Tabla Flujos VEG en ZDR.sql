
/****************
PROCESO A SEGUIR UNA VEZ, LA DE CREACIÓN Y DEFINICIÓN EN ÁREA DE RESULTADOS PARA TABLA DE INFO DE CLIENTES
DE DESCARGA DE CONSULTAS EN LA SSF
****************/

---PRIMERO CARGAR EL ARCHIVO DEL MES A CARGAR DE LA SSF 
--1. extraer la estructura de la tabla de proceso_bana_vbeyg.flujos_veg_tmp:
show create table proceso_bana_vbeyg.flujos_veg_tmp;

--2. modificar la estructura para resultados bana, agregando las columnas de ingestión y definiendo la partición:

CREATE TABLE resultados_bana_vbeyg.flujos_veg (
    ingestion_year INT,   
    ingestion_month INT,
    ingestion_day INT,   
    fecha_max_mes TIMESTAMP,   
    fecha_mes BIGINT,   
    numero_unico BIGINT,   
    nombre_cliente STRING,   
    cifcodejecuti BIGINT,   
    cifcodareafin BIGINT,   
    area_financiera STRING,   
    segmentobanca STRING,   
    subsegmentobanca STRING,   
    nombre_canal STRING,   
    accion_canal STRING,   
    contexto STRING,   
    tipo_trx STRING,   
    trxs BIGINT,   
    monto DOUBLE,   
    tipo_cartera STRING,   
    sector_ba STRING,   
    sub_sector_ba STRING,   
    actividad STRING,   
    cluster STRING ) 
partitioned  BY (
    year int 
)
STORED AS PARQUET ;


--3. agregar la partición al año 2020
alter table resultados_bana_vbeyg.flujos_veg 
add partition (year = 2019);

--4. por último, agregar los datos ya en la nueva tabla, a la partición respectiva: 
insert overwrite resultados_bana_vbeyg.flujos_veg 
partition (year = 2020)
select  year(now()) ingestion_year,
        month(now()) ingestion_month, 
        day(now()) ingestion_day,
        fecha_max_mes,
        fecha_mes,
        numero_unico,
        nombre_cliente,
        codciiu,
        cifcodejecuti,
        cifcodareafin,
        area_financiera,
        segmentobanca,
        subsegmentobanca,
        nombre_canal,
        accion_canal,
        contexto,
        tipo_trx,
        trxs,
        monto,
        no_master,
        nombre_grupo,
        tipo_cartera,
        sector_ba,
        sub_sector_ba,
        actividad,
        cluster,
        nivel_impacto,
        escenario_recuperacion
from proceso_bana_vbeyg.flujos_veg_tmp
where year(fecha_max_mes) = 2020;
