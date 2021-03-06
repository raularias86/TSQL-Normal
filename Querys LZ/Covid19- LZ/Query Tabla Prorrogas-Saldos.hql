--query tabla prorrogas con info coldocum de lz 
SELECT A.CIFCODCLIENTE UNICO, A.COLREFEREN NO_REFERENCIA, a.COLCICLOPGCA CICLO_PAGOS_CAP, 
a.COLCICLOPGINT CICLO_PAGOS_INT, B.DIAS_PRORROGA, B.REGISTRO_SISTEMAS, a.colmonto MONTO_REF,
a.colcuota, a.colfeape, a.colfeven, a.colplmeses 
FROM s_bana_productos.riesgodb_dwba_coldocum A 
JOIN proceso_bana_vbeyg.TABLA_PRORROGAS B ON A.colreferen = B.NO_REFERENCIA
WHERE YEAR(A.COLFCHPROC) = 2020 AND MONTH(A.COLFCHPROC) = 3
;

DROP TABLE PROCESO_BANA_VBEYG.saldos_activos_veg PURGE;

create table PROCESO_BANA_VBEYG.saldos_activos_veg stored as parquet as
WITH TABLA_PRORROGAS_INFOCOLDO AS (
    SELECT A.CIFCODCLIENTE UNICO, A.COLREFEREN NO_REFERENCIA, a.COLCICLOPGCA CICLO_PAGOS_CAP, 
    a.COLCICLOPGINT CICLO_PAGOS_INT, B.DIAS_PRORROGA, B.REGISTRO_SISTEMAS 
    --,a.colmonto MONTO_REF,a.colcuota, a.colfeape, a.colfeven, a.colplmeses 
    FROM s_bana_productos.riesgodb_dwba_coldocum A 
    JOIN proceso_bana_vbeyg.TABLA_PRORROGAS B ON A.colreferen = B.NO_REFERENCIA
    WHERE YEAR(A.COLFCHPROC) = 2020 AND MONTH(A.COLFCHPROC) = 4
), saldos_dash as (
    select  A.FCH_PROCESO, A.COD_AREA_FINANCIERA, A.area_financiera, A.COD_ASIGNADO, A.NO_UNICO, A.CLIENTE, 
    A.SEGMENT1, A.SEGMENT2, A.PRODUCTO, A.PLAZO_MESES, A.NO_CUENTA, A.STATUS, 
    A.FCH_APERTURA, A.FCH_VENCIMIENTO, A.SECTOR_ECONOMICO, A.DESTINO_BCR, A.SALDO SALDO_CONTABLE, A.CUOTA, 
    A.INTERES, A.CAPITAL_PAGADO_DIA, A.INTERES_PAGADO_DIA, A.DIAS_MORA_MAX,
    row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, 
    ingestion_month desc, ingestion_day desc) as rownum
    from s_bana_productos.basig_dashba_dash_portafolio_his a
    WHERE A.YEAR = 2020
    AND A.FCH_PROCESO >= '2020-01-01'
    AND A.SEGMENT1 = '1. PORTAFOLIO PRESTAMOS'
    AND A.STATUS != 'S'
    AND A.COD_aREA_FINANCIERA IN (1103,1106,1113,1112,1111,1120,1123,1101)
), dash_port as (
    select * 
    from saldos_dash 
    where rownum = 1 
)
    select  A.FCH_PROCESO, A.COD_AREA_FINANCIERA, A.area_financiera, A.COD_ASIGNADO, A.NO_UNICO, A.CLIENTE, 
    A.SEGMENT1, A.SEGMENT2, A.PRODUCTO, A.PLAZO_MESES, A.NO_CUENTA, A.STATUS, 
    A.FCH_APERTURA, A.FCH_VENCIMIENTO, A.SECTOR_ECONOMICO, A.DESTINO_BCR, A.SALDO_CONTABLE, A.CUOTA, 
    A.INTERES, A.CAPITAL_PAGADO_DIA, A.INTERES_PAGADO_DIA, A.DIAS_MORA_MAX,
    CASE WHEN B.NO_REFERENCIA IS NULL THEN 'Ref s/Prorroga' else 'Ref c/Prorroga' END AS FLAG_REFERENCIA,
    CASE WHEN B.UNICO IS NOT NULL THEN 'Cliente c/Prorroga' else 'Cliente s/Prorroga' END AS FLAG_CLIENTE,
    B.dias_prorroga, B.registro_sistemas, 
    B.CICLO_PAGOS_CAP, B.CICLO_PAGOS_INT
    from dash_port a 
    LEFT OUTER JOIN TABLA_PRORROGAS_INFOCOLDO B 
    ON A.no_cuenta = B.no_referencia AND A.no_unico = B.UNICO
;

compute stats PROCESO_BANA_VBEYG.saldos_activos_veg;
--------------------

--en bd local
SELECT A.FCH_PROCESO, A.COD_AREA_FINANCIERA, A.COD_ASIGNADO, A.NO_UNICO, A.NO_CUENTA, A.STATUS, 
A.FCH_APERTURA, A.FCH_VENCIMIENTO, A.SECTOR_ECONOMICO, A.DESTINO_BCR, A.SALDO_CONTABLE, A.CUOTA, 
A.INTERES, A.CAPITAL_PAGADO_DIA, A.INTERES_PAGADO_DIA, A.DIAS_MORA_MAX,	
CASE WHEN B.NO_REFERENCIA IS NULL THEN 'Ref s/Prorroga' else 'Ref c/Prorroga' END AS FLAG_REFERENCIA,
CASE WHEN B.NO_UNICO IS NOT NULL THEN 'Cliente c/Prorroga' else 'Cliente s/Prorroga' END AS FLAG_CLIENTE
--SELECT COUNT(1) 
FROM SALDOS.DBO.SALDOS_2017 A 
LEFT JOIN BASE_PRORROGAS_COVID19 B ON A.NO_CUENTA = B.NO_REFERENCIA 
AND A.NO_UNICO = B.NO_UNICO
WHERE A.FCH_PROCESO = '2020-01-01'
AND A.SEGMENT1 = '1. PORTAFOLIO PRESTAMOS'



