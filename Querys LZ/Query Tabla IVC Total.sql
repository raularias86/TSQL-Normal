drop table proceso_bana_vbeyg.vta_cruzada_total purge;

create table proceso_bana_vbeyg.vta_cruzada_total stored as parquet as 
with detalle_ebe as (
    SELECT a.fch_proceso, a.cod_area_financiera, a.cod_asignado, a.no_unico, 
           'EBanca Empresas' agrupador, 
           a.flag_ebanca producto_contratado, a.flag_activo producto_activo
    FROM proceso_bana_vbeyg.base_ebe a
    where a.fch_proceso >= '2019-01-31 00:00:00'
),  detalle_dash as (
    select a.fch_proceso, a.cod_area_financiera, a.cod_asignado, a.no_unico, 
           a.agrupacion_producto, 
           a.no_referencias_totales, a.no_referencias_activas
    from proceso_bana_vbeyg.vta_cruzado_dashport a 
    where a.fch_proceso is not null
    and a.fch_proceso >= '2019-01-31 00:00:00'
),  detalle_oprod as (
    select a.fch_proceso, a.cod_area_financiera, a.cod_asignado, a.no_unico, 
           a.agrupador, 
           count(1) referencias_totales, count(1) referencias_activas
    from proceso_bana_vbeyg.tabla_otros_productos a 
    where a.cod_area_financiera in (1103,1106,1113,1112,1111,1120,1123,1101)
    and a.fch_proceso >= '2019-01-31 00:00:00'
    group by a.fch_proceso, a.cod_area_financiera, a.cod_asignado, a.no_unico, 
           a.agrupador
)  
    select *
    from detalle_ebe
    union all
    select * 
    from detalle_dash
    union all
    select *
    from detalle_oprod
    order by fch_proceso, cod_area_financiera, cod_asignado, no_unico, agrupador
    ;
    
compute stats proceso_bana_vbeyg.vta_cruzada_total ;    