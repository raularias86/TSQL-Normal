--create table proceso_bana_vbeyg.autofondeo stored as parquet as

drop table proceso_bana_vbeyg.saldos_autoyrep_tmp1 purge;

create table proceso_bana_vbeyg.saldos_autoyrep_tmp1 stored as parquet as
    with  saldo_totales_dash as ( 
        select  a.fch_proceso, 
                year(a.fch_proceso)*100+month(a.fch_proceso) aniomes, 
                a.cod_area_financiera, 
                a.area_financiera, 
                a.cod_asignado, 
                a.no_unico, 
                a.cliente, 
                a.segment1, 
                a.segment2,
                a.no_cuenta, 
                status, 
                a.cod_producto, 
                a.saldo, 
        row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, 
        ingestion_month desc, ingestion_day desc) as rownum
        from s_bana_productos.basig_dashba_dash_portafolio_his a 
        where a.fch_proceso  >= '2019-01-01 00:00:00.0' and status != 'S'
        and cod_area_financiera in (1103,1106,1113,1112,1111,1120,1123,1101)
    ), saldos_dash_validados as (
        select * 
        from saldo_totales_dash
        where rownum = 1 and segment1 in ('1. PORTAFOLIO PRESTAMOS','PORTAFOLIO DEPOSITOS')
        --limit 100
    ) 
        select a.fch_proceso, a.aniomes, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.no_unico, a.cliente, 
        a.segment1, a.segment2,
        count(no_cuenta) referencias, 
        sum(saldo) saldo
        from saldos_dash_validados a
        group by fch_proceso, aniomes, cod_area_financiera, area_financiera, cod_asignado, no_unico, cliente, segment1, segment2

    ;     

compute stats proceso_bana_vbeyg.saldos_autoyrep_tmp1;

---------------------------/***************************************************************/------------------------

drop table proceso_bana_vbeyg.autofondeo_mensual purge;

create table proceso_bana_vbeyg.autofondeo_mensual stored as parquet as
with fechas_max as (
   select max(a.fch_proceso) fch_proceso --, --select distinct a.fch_proceso
    from proceso_bana_vbeyg.saldos_autoyrep_tmp1 a
    group by  year(a.fch_proceso), month(a.fch_proceso)
), saldos_fin_mes as (
    select a.*
    from proceso_bana_vbeyg.saldos_autoyrep_tmp1 a join fechas_max b 
    on a.fch_proceso = b.fch_proceso 
    --order by a.fch_proceso, a.no_unico
)
select a.fch_proceso, a.aniomes, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.segment1, a.segment2,
        sum(a.referencias) referencias, sum(a.saldo) saldo, count(distinct no_unico) clientes_x_prod
from saldos_fin_mes a
group by a.fch_proceso, a.aniomes, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.segment1, a.segment2
;

compute stats proceso_bana_vbeyg.autofondeo_mensual;

---------------------------/***************************************************************/------------------------

drop table proceso_bana_vbeyg.reciprocidad_mensual purge;

create table proceso_bana_vbeyg.reciprocidad_mensual stored as parquet as
with saldos_promedios as (
    select a.*,
           avg(a.saldo) over(partition by a.aniomes, a.no_unico, a.segment2 order by a.fch_proceso) saldo_promedio       
    from proceso_bana_vbeyg.saldos_autoyrep_tmp1 a 
),fechas_max as (
   select max(a.fch_proceso) fch_proceso --, --select distinct a.fch_proceso
    from proceso_bana_vbeyg.saldos_autoyrep_tmp1 a
    group by  year(a.fch_proceso), month(a.fch_proceso)
), saldos_prestamos_mes as (
    select a.fch_proceso, a.aniomes, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.no_unico,
            a.cliente, a.segment1, a.segment2, a.referencias, a.saldo, a.saldo_promedio
    from saldos_promedios a join fechas_max b 
    on a.fch_proceso = b.fch_proceso 
    where a.segment1 = '1. PORTAFOLIO PRESTAMOS'
    and a.saldo > 0 
), saldos_depositos_mes as (
    select a.fch_proceso, a.aniomes, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.no_unico,
            a.cliente, a.segment1, a.segment2, a.referencias, a.saldo, a.saldo_promedio
    from saldos_promedios a join fechas_max b 
    on a.fch_proceso = b.fch_proceso 
    where a.segment1 != '1. PORTAFOLIO PRESTAMOS'
    and a.saldo > 0 
    --limit 100
), depositos_deudores as (
    select a.*
    from saldos_depositos_mes a join saldos_prestamos_mes b
    on a.fch_proceso = b.fch_proceso and a.no_unico = b.no_unico
)   
    select * 
    from saldos_prestamos_mes
    union all
    select * 
    from depositos_deudores
;

compute stats proceso_bana_vbeyg.reciprocidad_mensual;

