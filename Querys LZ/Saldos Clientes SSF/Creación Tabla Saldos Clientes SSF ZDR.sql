/****************
PROCESO A SEGUIR UNA VEZ, LA DE CREACIÓN Y DEFINICIÓN EN ÁREA DE RESULTADOS PARA TABLA DE INFO DE CLIENTES
DE DESCARGA DE CONSULTAS EN LA SSF
****************/

---PRIMERO CARGAR EL ARCHIVO DEL MES A CARGAR DE LA SSF DESDE ARCHIVOS, DIRECCIÓN /user/ralvaren/Base_SSF_Clientes
--HACER EL PROCESO DE CREACIÓN DE TABLA TEMPORAL DEL ÚLTIMO ARCHIVO CARGADO, LUEGO INSERTAR EL NUEVO MES A TABLA: 
--proceso_bana_vbeyg.saldos_clientes_ssf: 
/* INSERT INTO proceso_bana_vbeyg.saldos_clientes_ssf
SELECT * 
FROM proceso_bana_vbeyg.saldos_clientes_ssf_tmp;

drop table proceso_bana_vbeyg.saldos_clientes_ssf_tmp purge;
*/
--1. extraer la estructura de la tabla de proceso_bana_vbeyg.saldos_clientes_ssf:
show create table proceso_bana_vbeyg.saldos_clientes_ssf;

--2. modificar la estructura para resultados bana, agregando las columnas de ingestión y definiendo la partición:

CREATE TABLE resultados_bana_vbeyg.saldos_clientes_ssf (
    ingestion_year INT,
    ingestion_month INT, 
    ingestion_day INT,
    fecha_corte TIMESTAMP,
    nit STRING,
    nombre_deudor STRING,
    no_unico BIGINT,
    nombre_cliente_ba STRING,
    vicepresidencia STRING,
    cod_area_financiera BIGINT,
    area_financiera STRING,
    cod_ejecutivo BIGINT,
    tipo_cliente STRING,
    tipo_cartera_asignada STRING,
    sector_ba STRING,
    sub_sector_ba STRING,
    actividad_ba STRING,
    cluster_ba STRING,
    codigo STRING,
    institucion STRING,
    tipo_institucion STRING,
    cod_destino_bcr STRING,
    descripcion_destino_bcr STRING,
    agrupador_sector_destino STRING,
    tipo_sector_destino_bcr STRING,
    tipo_cartera STRING,
    tipo_financiamiento STRING,
    no_referencia STRING,
    clasificacion_riesgo STRING,
    monto_otorgado DOUBLE,
    saldo_adeudado DOUBLE,
    saldo_vencido_capital DOUBLE,
    saldo_vigente_interes DOUBLE,
    saldo_vencido_interes DOUBLE,
    saldo_vigente_capital DOUBLE,
    dias_mora_capital INT,
    saldo_mora_capital DOUBLE,
    dias_mora_interes INT,
    saldo_mora_interes DOUBLE,
    tipo_prestamo STRING,
    total_riesgo DOUBLE,
    fecha_otorgado TIMESTAMP,
    fecha_vencimiento TIMESTAMP,
    fecha_castigo TIMESTAMP,
    estado STRING,
    interes_mensual DOUBLE,
    tasa_anual DOUBLE,
    tasa_efectiva DOUBLE,
    flag_estimacion_tasa STRING,
    flag_prorroga_co STRING
)
partitioned  BY (
    year int 
)
STORED AS PARQUET ;

--3. agregar la partición al año 2020
alter table resultados_bana_vbeyg.saldos_clientes_ssf 
add partition (year = 2018);

--4. por último, agregar los datos ya en la nueva tabla, a la partición respectiva: 
insert overwrite resultados_bana_vbeyg.saldos_clientes_ssf 
partition (year = 2020)
select  year(now()) ingestion_year,
        month(now()) ingestion_month, 
        day(now()) ingestion_day,
        fecha_corte,
        nit,
        nombre_deudor,
        no_unico,
        nombre_cliente_ba,
        vicepresidencia,
        cod_area_financiera,
        area_financiera,
        cod_ejecutivo,
        tipo_cliente,
        tipo_cartera_asignada,
        sector_ba,
        sub_sector_ba,
        actividad_ba,
        cluster_ba,
        codigo,
        institucion,
        tipo_institucion,
        cod_destino_bcr,
        descripcion_destino_bcr,
        agrupador_sector_destino,
        tipo_sector_destino_bcr,
        tipo_cartera,
        tipo_financiamiento,
        no_referencia,
        clasificacion_riesgo,
        monto_otorgado,
        saldo_adeudado,
        saldo_vencido_capital,
        saldo_vigente_interes,
        saldo_vencido_interes,
        saldo_vigente_capital,
        dias_mora_capital,
        saldo_mora_capital,
        dias_mora_interes,
        saldo_mora_interes,
        tipo_prestamo,
        total_riesgo,
        fecha_otorgado,
        fecha_vencimiento,
        fecha_castigo,
        estado,
        interes_mensual,
        tasa_anual,
        tasa_efectiva,
        flag_estimacion_tasa,
        flag_prorroga_co
from proceso_bana_vbeyg.saldos_clientes_ssf
where year(fecha_corte) = 2020;


COMPUTE stats resultados_bana_vbeyg.saldos_clientes_ssf;