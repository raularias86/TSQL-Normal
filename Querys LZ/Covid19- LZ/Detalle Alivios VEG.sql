drop table if exists proceso_bana_vbeyg.detalle_prorrogasveg purge;

create table proceso_bana_vbeyg.detalle_prorrogasveg stored as parquet as 
select  a.fecha_carga,
        'Referencia C/Prorroga CO' Tipo_referencia,
        a.no_unico, 
        b.cifnombreclie nombre_cliente,
        nvl(c.tipo_grupo,'I') tipo_grupo,
        nvl(c.no_unico_grupo,a.no_unico) no_master,
        nvl(c.nombre_grupo, b.cifnombreclie) nombre_grupo,
        a.cod_asignado, 
        a.cod_area_financiera, 
        a.area_financiera, 
        a.segmento_banca,
        nvl(d.sector_ba, 'S/D') sector_ba,
        nvl(d.sub_sector_ba,'S/D') sub_sector_ba,
        nvl(d.actividad_ba, 'S/D') actividad,
        nvl(d.cluster_ba, 'S/D') cluster,
        a.referencia,
        a.saldo_original,
        a.saldo_hoy,
        a.producto,
        a.codigo_producto, 
        a.nombre_producto, 
        a.ciclopago_k_hoy,
        a.ciclopago_i_hoy,
        a.fecha_alivio,
        a.fecha_apertura,
        a.fecha_vencimiento_ant,
        a.fecha_vencimiento_hoy,
        a.fecha_ultimo_abono_k,
        a.fecha_ultimo_abono_i,
        a.fecha_proxpago_k_hoy,
        a.fecha_proxpago_i_hoy, 
        a.meses_alivio_k,
        a.meses_alivio_i, 
        a.tasan_hoy,
        case when a.tasan_original <> a.tasan_hoy then 'Cambio Tasa' else 'Sin cambio en Tasa' end as Flag_cambio_tasan,
        a.dias_mora_original,
        a.dias_mora_hoy
        --select *--distinct ciclopago_k_hoy--, ciclopago_i_hoy
from proceso_bana.ca_covid_alivios a 
join s_bana_clientes.riesgodb_dwba_cifgenerales b on a.no_unico = b.cifcodcliente
left join proceso_bana_vbeyg.base_grupos_economicos c on a.no_unico = c.no_unico_cliente
left join proceso_bana_vbeyg.clientes_veg_mensual d on a.no_unico = d.no_unico 
where b.ciffechaproceso = '2020-06-30 00:00:00.0'
and a.cod_area_financiera in (1103,1106,1113,1112,1111,1120,1123,1101)
and a.codigo_producto = 11401
order by 1,6
;


compute stats proceso_bana_vbeyg.detalle_prorrogasveg;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

drop table if exists proceso_bana_vbeyg.co_saldos_activos purge;

create table proceso_bana_vbeyg.co_saldos_activos stored as parquet as
with saldos_dash as (
    select  a.fch_proceso, a.cod_area_financiera, a.area_financiera, a.cod_asignado, a.no_unico, a.cliente, 
    a.segment1, a.segment2, a.producto, a.plazo_meses, a.no_cuenta, a.status, 
    a.fch_apertura, a.fch_vencimiento, a.sector_economico, a.destino_bcr, a.saldo saldo_contable, a.cuota, 
    a.interes, a.capital_pagado_dia, a.interes_pagado_dia, a.dias_mora_max,
    row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, 
    ingestion_month desc, ingestion_day desc) as rownum --select *
    from s_bana_productos.basig_dashba_dash_portafolio_his a 
    where a.year = 2020
    and a.fch_proceso >= '2020-01-01 00:00:00.0'
    and a.segment1 = '1. PORTAFOLIO PRESTAMOS'
    and a.status != 'S'
    and a.cod_area_financiera in (1103,1106,1113,1112,1111,1120,1123,1101) 
), dash_port as (
    select * 
    from saldos_dash 
    where rownum = 1 
), flag_clientes_prorroga as (
    select distinct a.no_unico, 
           case when b.no_unico is null 
           then 'Cliente S/Prorroga' else 'Cliente C/Prorroga CO' 
           end as flag_cliente_prorroga
    from dash_port a 
    left join proceso_bana_vbeyg.detalle_prorrogasveg b on a.no_unico = b.no_unico 
)
select  a.fch_proceso, 
        a.cod_area_financiera, 
        a.area_financiera, 
        a.cod_asignado, 
        a.no_unico, 
        a.cliente,
        c.flag_cliente_prorroga,
        nvl(d.sector_ba, 'S/D') sector_ba,
        nvl(d.sub_sector_ba,'S/D') sub_sector_ba,
        nvl(d.actividad_ba, 'S/D') actividad,
        nvl(d.cluster_ba, 'S/D') cluster,        
        a.segment1, 
        a.segment2, 
        a.producto,
        nvl(tipo_referencia, 'Referencia S/Prorroga CO') as flag_referencia, 
        a.sector_economico, 
        a.destino_bcr, 
        count(a.no_cuenta) no_cuenta,         
        sum(a.saldo_contable) saldo_contable, 
        sum(a.capital_pagado_dia) capital_pagado_dia, 
        sum(a.interes_pagado_dia) interes_pagado_dia, 
        max(a.dias_mora_max) dias_mora_max
    from dash_port a 
    left outer join proceso_bana_vbeyg.detalle_prorrogasveg b 
    on a.no_cuenta = b.referencia and a.no_unico = b.no_unico
    left outer join proceso_bana_vbeyg.clientes_veg_mensual d
    on a.no_unico = d.no_unico
    join flag_clientes_prorroga c
    on a.no_unico = c.no_unico
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
    order by 1, 2, 4, 5
;

compute stats proceso_bana_vbeyg.co_saldos_activos;

------------ANTIGUO QUERY MÁS DETALLADO

select  a.fch_proceso, 
        a.cod_area_financiera, 
        a.area_financiera, 
        a.cod_asignado, 
        a.no_unico, 
        a.cliente,
        c.flag_cliente_prorroga,
        nvl(d.sector_ba, 'S/D') sector_ba,
        nvl(d.sub_sector_ba,'S/D') sub_sector_ba,
        nvl(d.actividad, 'S/D') actividad,
        nvl(d.cluster_ba, 'S/D') cluster,        
        a.segment1, 
        a.segment2, 
        a.producto, 
        a.plazo_meses, 
        a.no_cuenta, 
        a.status, 
        a.fch_apertura, 
        a.fch_vencimiento, 
        a.sector_economico, 
        a.destino_bcr, 
        a.saldo_contable, 
        a.cuota, 
        a.interes, 
        a.capital_pagado_dia, 
        a.interes_pagado_dia, 
        a.dias_mora_max,
        nvl(tipo_referencia, 'Referencia S/Prorroga CO') as flag_referencia,
        b.fecha_alivio,
        --case when b.no_unico is not null then 'cliente c/prorroga' else 'cliente s/prorroga' end as flag_cliente,
        b.meses_alivio_k meses_alivio_cap, 
        b.meses_alivio_i meses_alivio_int,
        --b.dias_prorroga, 
        'CO' registro_sistemas, 
        b.ciclopago_k_hoy ciclo_pagos_cap, 
        b.ciclopago_i_hoy ciclo_pagos_int    
    from dash_port a 
    left outer join proceso_bana_vbeyg.detalle_prorrogasveg b 
    on a.no_cuenta = b.referencia and a.no_unico = b.no_unico
    left outer join proceso_bana_vbeyg.clientes_veg_mensual d
    on a.no_unico = d.no_unico
    join flag_clientes_prorroga c
    on a.no_unico = c.no_unico
    order by 1, 2, 4, 5



;