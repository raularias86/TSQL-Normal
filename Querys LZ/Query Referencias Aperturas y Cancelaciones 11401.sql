create table proceso_bana_vbeyg.renovacion_cartera_comercial stored as parquet as 
with  saldo_totales_dash as ( 
    select a.fch_proceso, year(a.fch_proceso)*100+month(a.fch_proceso) aniomes, a.cod_area_financiera, a.cod_asignado,
    a.no_unico, cod_producto, a.no_cuenta, a.status, a.fch_apertura, a.fch_vencimiento, a.fch_cancelado, a.saldo, a.monto, --a.segment1,
    row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, 
    ingestion_month desc, ingestion_day desc) as rownum
    from s_bana_productos.basig_dashba_dash_portafolio_his a 
    where a.fch_proceso  >= '2019-01-01 00:00:00.0' --and status != 'S'
    and cod_area_financiera in (1103,1106,1113,1112,1111,1120,1123,1101)
    and cod_producto = 11401
), saldos_dash_validados as (
    select * 
    from saldo_totales_dash
    where rownum = 1 --and segment1 in ('1. PORTAFOLIO PRESTAMOS','PORTAFOLIO DEPOSITOS')
    --limit 100
), referencias_vencidas as (
    select 'Vencida' tipo, *
    from saldos_dash_validados a
    where a.status != 'S' 
    and a.fch_proceso = a.fch_vencimiento 
), referencias_nuevas as (
    select 'Apertura' tipo, *
    from saldos_dash_validados a
    where a.fch_proceso = a.fch_apertura 
), referencias_canceladas as (
    select 'Cancelada' tipo, *
    from saldos_dash_validados a
    where a.fch_proceso = a.fch_cancelado 
)
select *
from referencias_vencidas
union all 
select * 
from referencias_nuevas 
union all
select * 
from referencias_Canceladas
--where 
; 

compute stats proceso_bana_vbeyg.renovacion_cartera_comercial;

drop table  proceso_bana_vbeyg.renovacion_cartera_comercial;


select * from proceso_bana_vbeyg.renovacion_cartera_comercial 
order by 3,2,5 ;