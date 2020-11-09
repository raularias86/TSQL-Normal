/*********ACTIVOS-PASIVOS VEG DESDE LZ *******/

create table proceso_bana_vbeyg.saldos_veg_dash stored as parquet as 
with
dash_ba as
(
    select
        FCH_PROCESO, NO_UNICO, CLIENTE, COD_ASIGNADO, COD_AREA_FINANCIERA,
        SEGMENT1, SEGMENT2 ,NO_CUENTA, PRODUCTO, COD_PRODUCTO, MONTO, PLAZO_MESES, TASA_INTERES,
        FCH_APERTURA, FCH_VENCIMIENTO, SECTOR_ECONOMICO,DESTINO_BCR, SALDO, saldo_referencia, CUOTA,
        INTERES, CAPITAL_PAGADO_DIA, INTERES_PAGADO_DIA, DIAS_MORA_MAX, STATUS,
        row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, ingestion_month desc, ingestion_day desc) as rownum 
    from s_bana_productos.basig_dashba_dash_portafolio_his
    where
        fch_proceso >= '2019-01-01 00:00:00.0'
        AND ( STATUS != 'S')
        AND ( COD_AREA_FINANCIERA IN (1103,1106,1113,1112,1111,1120,1123,1101))
)
select 
        * 
from dash_ba 
where rownum = 1

