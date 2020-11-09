CREATE EXTERNAL TABLE proceso_bana_vbeyg.tabla_saldos_ssf_clientes_tmp (
        ROL STRING,
        NIT STRING,
        NOMBRE_DEUDOR STRING,
        FECHA_CORTE STRING,
        CODIGO STRING,
        INSTITUCION STRING,
        TIPO_INSTITUCION STRING,
        COD_DESTINO_BCR STRING,
        DESCRIPCION_DESTINO_BCR STRING,
        DEUDOR	STRING,
        FECHA_CORTEN	STRING,
        COD_INSTITUCION	STRING,
        TIPO_CARTERA STRING,
        TIPO_FINANCIAMIENTO STRING,
        NO_REFERENCIA STRING,
        CLASIFICACION_RIESGO STRING,
        MONTO_OTORGADO DOUBLE,
        SALDO_ADEUDADO DOUBLE,
        SALDO_VENCIDO_CAPITAL DOUBLE,
        SALDO_VIGENTE_INTERES DOUBLE,
        SALDO_VENCIDO_INTERES DOUBLE,
        SALDO_VIGENTE_CAPITAL DOUBLE,
        DIAS_MORA_CAPITAL INT,
        SALDO_MORA_CAPITAL DOUBLE,
        DIAS_MORA_INTERES INT,
        SALDO_MORA_INTERES DOUBLE,
        TIPO_PRESTAMO STRING,
        TOTAL_RIESGO DOUBLE,
        FECHA_OTORGADO STRING,
        FECHA_VENCIMIENTO STRING,
        FECHA_CASTIGO STRING,
        categoria	STRING,
        ESTADO STRING
) -- Especificar campos y tipos de datos que deben coincidir con el archivo plano que se va a cargar.
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\|' -- Especificar separador de columnas
LOCATION '/user/ralvaren/Base_SSF_Clientes/2020' -- Especificar carpeta HDFS donde se encuentra el archivo o archivos planos
TBLPROPERTIES ("skip.header.line.count"="1")
; -- Esta línea va sólo si en el plano se encuentra el header en la primera línea.


DROP TABLE proceso_bana_vbeyg.tabla_saldos_ssf_clientes_2020 PURGE; -- Eliminar tabla final si ya existe

CREATE TABLE proceso_bana_vbeyg.tabla_saldos_ssf_clientes_2020 STORED AS PARQUET AS 
SELECT * FROM proceso_bana_vbeyg.tabla_saldos_ssf_clientes_tmp; 

compute stats  proceso_bana_vbeyg.tabla_saldos_ssf_clientes_2020;

-- DROP TABLE proceso_bana_vbeyg.tabla_saldos_ssf_clientes_tmp PURGE; -- Eliminar tabla temporal

-- --PARA INSERTAR LOS ÚLTIMOS VALORES EN LA TABLA DE SALDOS QUE SE CARGA A RESULTADOS
-- insert into tabla_saldos_ssf_clientes
-- select *
-- from proceso_bana_vbeyg.tabla_saldos_ssf_clientes_2020
-- where fecha_corte in ('2020-05-31','2020-06-30')
-- ;

