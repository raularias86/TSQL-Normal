
drop table if exists proceso_bana.flujos_empresas_tmp purge;


create table proceso_bana.flujos_empresas_tmp stored as parquet as
with all_trxs as (
select to_timestamp(cast(sk_fecha_transaccion as string),'yyyyMMdd') fecha_transaccion,
       agrupacion_sk_cliente_destino,
       monto
from proceso_bana_vghi.veg_huella_trx a
where sk_fecha_transaccion between 20190101 and year(now())*10000+month(now())*100+day(now())-1
  --and agrupacion_sk_cliente_destino=899975
),
cifgen as (
select
        cifcodcliente as no_unico,
        cifnombreclie as nombre_cliente,
        cifcodareafin
    from s_bana_clientes.riesgodb_dwba_cifgenerales
    where ciffechaproceso = '2020-04-30 00:00:00.0'
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
)

select year(fecha_transaccion)*100+month(fecha_transaccion) fecha_mes,
        agrupacion_sk_cliente_destino no_unico, 
        b.nombre_cliente,
        b.cifcodareafin,
        c.findesareafin area_financiera,
        iNITCAP(SECBA.FINSECTORBANCA) segmentobanca, 
        INITCAP(SECBA.FINSUBGRUPO) SubSegmentoBanca,
        sum(monto) monto
from all_trxs a
left join cifgen b on a.agrupacion_sk_cliente_destino=b.no_unico
left join catareafinanc c on b.cifcodareafin=c.FINCODAREAFIN
LEFT JOIN catsectorbanca SECBA ON SECBA.FINCODSECTORBCA=c.FINCODSECTORBANCA
WHERE day(fecha_transaccion) between 1 and day(now())
group by 1,2,3,4,5,6,7
order by 2,1 desc
;

compute stats proceso_bana.flujos_empresas_tmp;


drop table if exists proceso_bana.flujos_empresas_tmp2 purge;


create table proceso_bana.flujos_empresas_tmp2 stored as parquet as


with actividad_econo as (
    select
        numero_cliente as no_unico,
        cluster,
        sector_ba
    from proceso_bana_vghi.catalogo_sectores_empresas
)

select c.cluster,
        c.sector_ba,
        b.cod_master,
        b.grupo_economico,
        b.tipo_institucion,
        a.*
from proceso_bana.flujos_empresas_tmp a
left join proceso_bana_vghi.universo_privado_huella b on a.no_unico=b.no_unico
left join actividad_econo c on a.no_unico=c.no_unico;


compute stats proceso_bana.flujos_empresas_tmp2;


drop table if exists proceso_bana.flujos_empresas_tmp3 purge;

create table proceso_bana.flujos_empresas_tmp3 stored as parquet as
--with flag_empresas as (
--select numero_unico from  proceso_bana_vghi.estudio_covid19_empresas
--where identificacion_clientes='OK'
--),
--flag_pyme as (
--select no_unico from proceso_bana_vghi.riesgo_pyme_covid19
--)
--select a.* ,
--case when b.numero_unico is not null then 1 else 0 end flag_riesgo_empresa,
--case when c.no_unico is not null then 1 else 0 end flag_riesgo_pyme
--from  proceso_bana.flujos_empresas_tmp2 a 
--left join flag_empresas b on b.numero_unico=a.no_unico
--left join flag_pyme c on c.no_unico=a.no_unico

select a.* ,
        0 flag_riesgo_empresa,
        0 flag_riesgo_pyme
from  proceso_bana.flujos_empresas_tmp2 a 
;

compute stats proceso_bana.flujos_empresas_tmp3;


drop table if exists proceso_bana.flujos_empresas_tmp4 purge;


create table proceso_bana.flujos_empresas_tmp4 stored as parquet as
SELECT cluster,
        sector_ba,
        cod_master,
        grupo_economico,
        tipo_institucion,
        no_unico,
        nombre_cliente,
        cifcodareafin,
        area_financiera,
        segmentobanca,
        subsegmentobanca,
        sum(case when fecha_mes=201901 then monto else 0 end) flujo_201901,
        sum(case when fecha_mes=201902 then monto else 0 end) flujo_201902,
        sum(case when fecha_mes=201903 then monto else 0 end) flujo_201903,
        sum(case when fecha_mes=201904 then monto else 0 end) flujo_201904,
        sum(case when fecha_mes=201905 then monto else 0 end) flujo_201905,
        sum(case when fecha_mes=201906 then monto else 0 end) flujo_201906,
        sum(case when fecha_mes=201907 then monto else 0 end) flujo_201907,
        sum(case when fecha_mes=201908 then monto else 0 end) flujo_201908,
        sum(case when fecha_mes=201909 then monto else 0 end) flujo_201909,
        sum(case when fecha_mes=201910 then monto else 0 end) flujo_201910,
        sum(case when fecha_mes=201911 then monto else 0 end) flujo_201911,
        sum(case when fecha_mes=201912 then monto else 0 end) flujo_201912,
        sum(case when fecha_mes=202001 then monto else 0 end) flujo_202001,
        sum(case when fecha_mes=202002 then monto else 0 end) flujo_202002,
        sum(case when fecha_mes=202003 then monto else 0 end) flujo_202003,
        sum(case when fecha_mes=202004 then monto else 0 end) flujo_202004,
        sum(case when fecha_mes=202005 then monto else 0 end) flujo_202005

FROM proceso_bana.flujos_empresas_tmp3
group by 1,2,3,4,5,6,7,8,9,10,11;

compute stats proceso_bana.flujos_empresas_tmp4;
