/**********************versión 2 ****************************/

drop table proceso_bana_vbeyg.vta_cruzado_dashport_tmp1 purge ;

create table proceso_bana_vbeyg.vta_cruzado_dashport_tmp1 stored as parquet as
with  saldo_totales_dash as ( 
    select a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, 
    a.no_unico, a.cliente, a.segment1, a.no_cuenta, status, a.cod_producto, a.saldo, 
    row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, 
    ingestion_month desc, ingestion_day desc) as rownum
    from s_bana_productos.basig_dashba_dash_portafolio_his a 
    where a.fch_proceso  >= '2019-01-01 00:00:00.0' and status != 'S'
    and cod_area_financiera in (1103,1106,1113,1112,1111,1120,1123,1101)
)  --, saldos_dash_validados as (
    select * 
    from saldo_totales_dash
    where rownum = 1 and segment1 in ('1. PORTAFOLIO PRESTAMOS','PORTAFOLIO DEPOSITOS') 
;    
    
compute stats proceso_bana_vbeyg.vta_cruzado_dashport_tmp1 ;  

create table proceso_bana_vbeyg.vta_cruzado_dashport stored as parquet as
with fechas_fin_mes as ( 
    select max(a.fch_proceso) fch_proceso
    from proceso_bana_vbeyg.vta_cruzado_dashport_tmp1 a
    group by year(a.fch_proceso), month(a.fch_proceso)
), saldos_dash as (
    select a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado,  
    a.no_unico, a.cliente, a.segment1 as tipo_producto, a.no_cuenta, status, a.cod_producto, a.saldo
    from proceso_bana_vbeyg.vta_cruzado_dashport_tmp1 a join fechas_fin_mes b 
    on a.fch_proceso = b.fch_proceso 
), catalogo_productos as (
    select a.colcodproducto, a.colagrupaprod,
    row_number() over (partition by a.colcodproducto, a.colagrupaprod order by a.ingestion_year desc, 
    a.ingestion_month desc, a.ingestion_day desc) as rownum
    from s_bana_productos.basig_bacatalog_catproductos a 
), catalogo_val as (
    select * 
    from catalogo_productos 
    where rownum = 1
), productos_totales as (
   select a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, 
       a.no_unico, a.cliente, a.tipo_producto, 
       case when a.cod_producto in (20106,20132,20107,20108,20117,20167,20168,20134) then 'CASH MANAGER'
       ELSE b.colagrupaprod end as agrupacion_producto, 
   count(1) No_Referencias_Totales
   from saldos_dash a JOIN catalogo_val b 
   on a.cod_producto = b.colcodproducto
   group by a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado,  
       a.no_unico, a.cliente, a.tipo_producto,
       case when a.cod_producto in (20106,20132,20107,20108,20117,20167,20168,20134) then 'CASH MANAGER'
       ELSE b.colagrupaprod end
), productos_activos as (
   select a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, 
       a.no_unico, a.cliente, a.tipo_producto, 
       case when a.cod_producto in (20106,20132,20107,20108,20117,20167,20168,20134) then 'CASH MANAGER'
       ELSE b.colagrupaprod end as agrupacion_producto, 
       count(1) No_Referencias_activas
   from saldos_dash a JOIN catalogo_val b 
   on a.cod_producto = b.colcodproducto
   where a.status = 'A'
   group by a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado,
       a.no_unico, a.cliente, a.tipo_producto, 
       case when a.cod_producto in (20106,20132,20107,20108,20117,20167,20168,20134) then 'CASH MANAGER'
       ELSE b.colagrupaprod end
)

select a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, 
       a.no_unico, a.cliente, a.tipo_producto,  a.agrupacion_producto, 
       a.no_referencias_totales, nvl(b.no_referencias_activas,0) no_referencias_activas
from productos_totales a left outer join productos_activos b 
on a.fch_proceso = b.fch_proceso and a.no_unico = b.no_unico and a.agrupacion_producto = b.agrupacion_producto
order by a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, 
         a.no_unico, a.cliente, a.tipo_producto, a.agrupacion_producto
;

compute stats proceso_bana_vbeyg.vta_cruzado_dashport;

drop table proceso_bana_vbeyg.vta_cruzado_dashport_tmp1 purge;

/*************versión 1 **********************/

create table proceso_bana_vbeyg.vta_cruzado_dashport stored as parquet as
with  saldo_totales_dash as ( 
    select a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.ejecutivo, 
    a.no_unico, a.cliente, a.segment1, a.no_cuenta, status, a.cod_producto, a.saldo, 
    row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, 
    ingestion_month desc, ingestion_day desc) as rownum
    from s_bana_productos.basig_dashba_dash_portafolio_his a 
    where a.fch_proceso  >= '2019-01-01 00:00:00.0'
), saldos_dash_validados as (
    select * 
    from saldo_totales_dash
    where rownum = 1 and segment1 = '1. PORTAFOLIO PRESTAMOS' and status != 'S'
    and cod_area_financiera in (1103,1106,1113,1112,1111,1120,1123,1101)
), fechas_fin_mes as ( 
    select max(a.fch_proceso) fch_proceso
    from saldos_dash_validados a
    group by year(a.fch_proceso), month(a.fch_proceso)
), saldos_dash as (
    select a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.ejecutivo, 
    a.no_unico, a.cliente, a.segment1 as tipo_producto, a.no_cuenta, status, a.cod_producto, a.saldo
    from saldos_dash_validados a join fechas_fin_mes b 
    on a.fch_proceso = b.fch_proceso 
),catalogo_productos as (
    select a.colcodproducto, a.colnomproducto, a.colagrupaprod,
    row_number() over (partition by a.colcodproducto, a.colnomproducto, a.colagrupaprod order by a.ingestion_year desc, 
    a.ingestion_month desc, a.ingestion_day desc) as rownum
    from s_bana_productos.basig_bacatalog_catproductos a 
), catalogo_val as (
    select * 
    from catalogo_productos 
    where rownum = 1
), productos_totales as (
   select a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.ejecutivo, 
       a.no_unico, a.cliente, a.tipo_producto, 
       case when a.cod_producto in (20106,20132,20107,20108,20117,20167,20168,20134) then 'CASH MANAGER'
       ELSE b.colagrupaprod end as agrupacion_producto, 
   count(1) No_Referencias_Totales
   from saldos_dash a JOIN catalogo_val b 
   on a.cod_producto = b.colcodproducto
   group by a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.ejecutivo, 
       a.no_unico, a.cliente, a.tipo_producto,
       case when a.cod_producto in (20106,20132,20107,20108,20117,20167,20168,20134) then 'CASH MANAGER'
       ELSE b.colagrupaprod end
), productos_activos as (
   select .fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.ejecutivo, 
       a.no_unico, a.cliente, a.tipo_producto, 
       case when a.cod_producto in (20106,20132,20107,20108,20117,20167,20168,20134) then 'CASH MANAGER'
       ELSE b.colagrupaprod end as agrupacion_producto, 
       count(1) No_Referencias_activas
   from saldos_dash a JOIN catalogo_val b 
   on a.cod_producto = b.colcodproducto
   where a.status = 'A'
   group by .fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.ejecutivo, 
       a.no_unico, a.cliente, a.tipo_producto, 
       case when a.cod_producto in (20106,20132,20107,20108,20117,20167,20168,20134) then 'CASH MANAGER'
       ELSE b.colagrupaprod end
)
select .fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.ejecutivo, 
       a.no_unico, a.cliente, a.tipo_producto,  a.agrupacion_producto, 
       a.no_referencias_totales, nvl(b.no_referencias_activas,0) no_referencias_activas
from productos_totales a left outer join productos_activos b 
on a.fch_proceso = b.fch_proceso and a.no_unico = b.no_unico and a.agrupacion_producto = b.agrupacion_producto
;

compute stats proceso_bana_vbeyg.vta_cruzado_dashport;
