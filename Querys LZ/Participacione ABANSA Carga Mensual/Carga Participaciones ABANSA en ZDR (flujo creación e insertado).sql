
--1. extraer la estructura de la tabla de proceso_bana_vbeyg.catalogo_sectorial:
show create table proceso_bana_vbeyg.participaciones_abansa;

--2. Agregar las columnas de ingestión 
CREATE TABLE resultados_bana_vbeyg.participaciones_abansa (
    ingestion_year INT,   
    ingestion_month INT,
    ingestion_day INT,
    fecha TIMESTAMP,   
    indicador STRING,   
    institucion STRING,   
    saldo DOUBLE,   
    saldo_mes_anterior DOUBLE 
) 
partitioned  BY (
    year int 
)
STORED AS PARQUET ;

--3. agregar la partición desde el año 2008 a 2020 (ir cambiando el year al que se agregará)
alter table resultados_bana_vbeyg.participaciones_abansa 
add partition (year = 2020);

--4. Agregado de los datos según las particiones: 
insert into resultados_bana_vbeyg.participaciones_abansa 
partition (year = 2020)
select  year(now()) ingestion_year,
        month(now()) ingestion_month, 
        day(now()) ingestion_day,
        fecha,
        indicador,
        institucion, 
        saldo,
        saldo_mes_anterior
from proceso_bana_vbeyg.participaciones_abansa 
where year(fecha) = 2020
and MONTH(fecha) = 6
order by 4,5,6
;