drop table proceso_bana_vbeyg.analisis_actividades_tmp purge;


CREATE TABLE proceso_bana_vbeyg.analisis_actividades_tmp stored as PARQUET as 
with all_trxs as (
select to_timestamp(cast(sk_fecha_transaccion as string),'yyyyMMdd') fecha_transaccion,
       'Ingreso' Tipo_trx,
       a.contexto,
       a.nombre_canal,
       agrupacion_sk_cliente_destino numero_unico,
       monto
from proceso_bana.veg_huella_trx a
where sk_fecha_transaccion between 20190101 and year(now())*10000+month(now())*100+day(now())-1
--group by 1,2,3,4,5
union all
select to_timestamp(cast(sk_fecha_transaccion as string),'yyyyMMdd') fecha_transaccion,
       'Salida' Tipso_trx,
       a.contexto,
       a.nombre_canal,
       agrupacion_sk_cliente_origen numero_unico,
       monto
from proceso_bana.veg_huella_trx a
where sk_fecha_transaccion between 20190101 and year(now())*10000+month(now())*100+day(now())-1
--group by  1,2,3,4,5
union all 
select  a.fch_proc, 
        'Ingreso' Tipo_trx,
        'Entrada' contexto,
        'ACH' Canal,
        b.capcodcliente numunico,
        a.tljbca monto
from s_bana_productos.basig_stageba_tljrnm_hist a
join s_bana_productos.riesgodb_dwba_capcuentas b 
    on a.tljoac = b.capnumcuenta 
where tljnem like 'ACH '
    and a.tljttp = 'DP'
    and a.fch_proc >= '2019-01-01 00:00:00.0'
    and b.capfchproceso = '2020-06-30 00:00:00.0'            
),
cifgen as (
select
        cifcodcliente as no_unico,
        cifnombreclie as nombre_cliente,
        lpad(cast(cifcodactivid as string),6,"0") as codciiu,
        cifcodejecuti,
        cifcodareafin   --select *
    from s_bana_clientes.riesgodb_dwba_cifgenerales
    where ciffechaproceso = '2020-10-31 00:00:00.0'
)
, catareafinanc as (
select *
from s_bana_productos.basig_bacatalog_catareafinanc 
where ingestion_year=2020
  and ingestion_month=3
  and ingestion_day=1

)
, catsectorbanca as (
select *
from s_bana_productos.basig_bacatalog_catsectorbanca 
where ingestion_year=2020
  and ingestion_month=2
  and ingestion_day=26
), 
fechas_fin_mes as (
    select year(fecha_transaccion) anio, month(fecha_transaccion) mes, 
    year(fecha_transaccion)*100+month(fecha_transaccion) fecha_mes,
    max(fecha_transaccion) fecha_max_mes 
    from all_trxs
    group by 1,2
)
, detalle_trx as (
select year(fecha_transaccion)*100+month(fecha_transaccion) fecha_mes,
        a.numero_unico, 
        b.nombre_cliente,
        b.codciiu,
        cifcodejecuti,
        b.cifcodareafin cod_area_financiera,
        c.findesareafin area_financiera,
        INITCAP(SECBA.FINSECTORBANCA) segmentobanca, 
        INITCAP(SECBA.FINSUBGRUPO) SubSegmentoBanca,
        Tipo_trx,
        a.contexto,
        count(1) trxs,
        sum(monto) monto
from all_trxs a
left join cifgen b on a.numero_unico =b.no_unico
left join catareafinanc c on b.cifcodareafin=c.FINCODAREAFIN
LEFT JOIN catsectorbanca SECBA ON SECBA.FINCODSECTORBCA=c.FINCODSECTORBANCA
where 
b.cifcodareafin in (1103,1106,1113,1112,1111,1120,1123,1101,1241,1242,1243,1230,1244)
--(1103,1106,1113,1112,1111,1120,1123,1101)
-- (1103,1106,1113,1112,1111,1120,1123,1101,1241,1242,1243,1230,1244)
--and a.numero_unico = 21898
group by 1,2,3,4,5,6,7,8,9,10,11--,12
) --order by 10,8 desc
, sectores_veg as (
    select  a.no_unico, 
            a.tipo_cartera,
            a.sector_ba, 
            a.sub_sector_ba, 
            a.actividad, 
            a.cluster_ba
    from proceso_bana_vbeyg.clientes_veg_mensual a
)
select  b.fecha_max_mes, a.*,
        nvl(c.tipo_cartera,"Asignado") tipo_cartera,
        nvl(c.sector_ba,"ND") sector_ba,
        nvl(c.sub_sector_ba,"ND") sub_sector_ba,
        nvl(c.actividad, "ND") actividad,
        nvl(c.cluster_ba,"ND") cluster
from detalle_trx a 
join fechas_fin_mes b on a.fecha_mes = b.fecha_mes
left outer join sectores_veg c on a.numero_unico = c.no_unico
where a.cod_area_financiera in (1103,1106,1113,1112,1111,1120,1123,1101)
--order by 1,2,3,4,5,6,7,8,9,10,11,12
UNION ALL
select b.fecha_max_mes, a.*,
        NULL tipo_cartera,
        nvl(c.sector_ba,"ND") sector_ba,
        nvl(c.sub_sector_ba,"ND") sub_sector_ba,
        nvl(c.actividad_ba, "ND") actividad,
        nvl(c.cluster_ba,"ND") cluster        
from detalle_trx a 
join fechas_fin_mes b on a.fecha_mes = b.fecha_mes
left join resultados_bana_vbeyg.catalogo_sectorial_ba c on a.codciiu = c.cod_portal
where  a.cod_area_financiera in (1241,1242,1243,1230,1244)
AND c.fch_catalogo = '2020-06-30 00:00:00.0'
;


----------INGRESOS VÍA ACH: ------------
create table proceso_bana_vbeyg.transcciones_alternas_tmp stored as parquet as 
select  a.fch_proc, 
        'Ingreso' Tipo_trx,
        'Entrada ACH' contexto,
        'ACH' Canal,
        b.capcodcliente numunico,
        a.traamt monto
from s_bana_productos.basig_dwba_ttran_hist_part a
join s_bana_productos.riesgodb_dwba_capcuentas b 
    on a.traoac = b.capnumcuenta
where a.tranem like 'ACH '
    and a.tracde = 'DP'
    and a.fch_proc >= '2019-01-01 00:00:00.0'
    and b.capfchproceso = '2020-06-30 00:00:00.0'
--group by 1,2,3,4,5
union ALL
select  fch_proc, 
        'Ingreso' Tipo_trx,
        'Entrada Transferencia Intl' contexto,
        'SWIFT' canal,
        b.capcodcliente numunico,
        case when a.traamt < a.tranet then a.tranet else a.tranem end as monto
from s_bana_productos.basig_dwba_ttran_hist_part a
join s_bana_productos.riesgodb_dwba_capcuentas b 
    on a.traoac = b.capnumcuenta
WHERE TRACDE = 'WD' --ORDER BY TRANEM;
and a.fch_proc >= '2019-01-01 00:00:00.0'
and b.capfchproceso = '2020-06-30 00:00:00.0'
union ALL

select  fch_proc, 
        'Salida' Tipo_trx,
        'Salida Transferencia Intl' contexto,
        'SWIFT' canal,
        b.capcodcliente numunico,
        case when a.traamt < a.tranet then a.tranet else a.tranem end as monto
from s_bana_productos.basig_dwba_ttran_hist_part a
join s_bana_productos.riesgodb_dwba_capcuentas b 
    on a.traoac = b.capnumcuenta
WHERE TRACDE = 'WC' --ORDER BY TRANEM;
and fch_proc >= '2019-01-01 00:00:00.0'
and b.capfchproceso = '2020-06-30 00:00:00.0'
;
