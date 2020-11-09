drop table if exists proceso_bana_vbeyg.ca_covid19_ar_tmp  purge;

--primero se trae el detalle de los código de AR y único, exceptuando los indicados más abajo
create table proceso_bana_vbeyg.ca_covid19_ar_tmp stored as parquet as 
with agrmst as (
select AGRCOD,AGRCUN
from s_bana_productos.basig_stageba_agrmst_stage
where ingestion_year=2020
  and ingestion_month=3
  and ingestion_day=30
  and AGRCUN<>0
)
select cifcodcliente no_unico,
       colfchproc fecha_proceso,
       b.agrcun empresa_ar,
       sum(colsaldo) saldo, 
       case when max(a.coldiasmor) = 0 then '1. SIN MORA'
            WHEN MAX(a.coldiasmor) <= 30 THEN '2. MORA 30'
            ELSE '3. MORA +30' END AS dias_mora
from s_bana_productos.riesgodb_dwba_coldocum a
inner join agrmst b on a.colagente=b.agrcod
where colfchproc >= '2019-01-01 00:00:00.0'
  and trim(colagente) is not null
  and cast(colagente as int) not in (0,1,8000,10014) --1 = BA, 8000 = SIN AR, 10014 = CON OPI SIN AGENTE DE RETENCION
  and colsaldo>0
  and colcodtiposaldo<>3
group by 1,2,3
;
compute stats proceso_bana_vbeyg.ca_covid19_ar_tmp ;

select * from proceso_bana_vbeyg.ca_covid19_ar_tmp limit 100;


drop table if exists proceso_bana_vbeyg.ca_covid19_ar_tmp2  purge;

create table proceso_bana_vbeyg.ca_covid19_ar_tmp2 stored as parquet as 
with  empresas_ar as (
    select a.fecha_proceso, a.empresa_ar, COUNT(NO_UNICO) empleados, SUM(SALDO) saldo_empleados
    from proceso_bana_vbeyg.ca_covid19_ar_tmp a
    --where a.EMPRESA_AR = 1523
    group by a.fecha_proceso, a.empresa_ar
), clientes_stg1 as (
    select a.fecha_proceso, a.empresa_ar, COUNT(NO_UNICO) EMPLEADOS_SIN_MORA, SUM(SALDO) SALDO_SIN_MORA
    from proceso_bana_vbeyg.ca_covid19_ar_tmp a
    where a.dias_mora = '1. SIN MORA'
    --AND a.EMPRESA_AR = 1523
    group by a.fecha_proceso, a.empresa_ar
), clientes_stg2 as (
    select a.fecha_proceso, a.empresa_ar, COUNT(NO_UNICO) EMPLEADOS_MORA_30, SUM(SALDO) SALDO_MORA_30
    from proceso_bana_vbeyg.ca_covid19_ar_tmp a
    where a.dias_mora = '2. MORA 30'
    --AND a.EMPRESA_AR = 1523
    group by a.fecha_proceso, a.empresa_ar 
), clientes_stg3 AS (
    select a.fecha_proceso, a.empresa_ar, COUNT(NO_UNICO) EMPLEADOS_MORA_MAS30, SUM(SALDO) SALDO_MORA_MAS30
    from proceso_bana_vbeyg.ca_covid19_ar_tmp a
    where a.dias_mora = '3. MORA +30'
    --AND a.EMPRESA_AR = 1523
    group by a.fecha_proceso, a.empresa_ar
)
SELECT a.fecha_proceso, a.empresa_ar, empleados, saldo_empleados, 
       nvl(b.empleados_sin_mora,0) empleados_sin_mora, 
       nvl(b.saldo_sin_mora, 0) saldo_sin_mora, nvl(c.empleados_mora_30,0) empleados_mora_30, 
       nvl(c.saldo_mora_30,0) saldo_mora, nvl(d.empleados_mora_mas30,0) empleados_mora_mas30, 
       nvl(d.saldo_mora_mas30,0) saldo_mora_mas30
FROM empresas_ar a 
left join clientes_stg1 b on a.fecha_proceso = b.fecha_proceso and a.empresa_ar = b.empresa_ar
left join clientes_stg2 c on a.fecha_proceso = c.fecha_proceso and a.empresa_ar = c.empresa_ar
left join clientes_stg3 d on a.fecha_proceso = d.fecha_proceso and a.empresa_ar = d.empresa_ar
--where a.empresa_ar = 1523
;

compute stats proceso_bana_vbeyg.ca_covid19_ar_tmp2 ;

create table proceso_bana_vbeyg.ca_covid19_empleador stored as parquet as
with unicos_empresasar as (
    select fecha_proceso, empresa_ar
    from proceso_bana_vbeyg.ca_covid19_ar_tmp2
), saldos_empresas as (
    select a.fch_proceso, a.no_unico, a.cliente, a.no_cuenta, a.cod_producto, a.saldo, a.dias_mora_max, 
    row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, 
    ingestion_month desc, ingestion_day desc) as rownum
    from s_bana_productos.basig_dashba_dash_portafolio_his a 
    join unicos_empresasar c on a.fch_proceso = c.fecha_proceso 
    and a.no_unico = c.empresa_ar 
    where a.segment1 = '1. PORTAFOLIO PRESTAMOS' and status != 'S'
), saldos_dash as (
    select fch_proceso, no_unico, cliente, 
    count(no_cuenta) referencias_empresa, sum(saldo) saldo_empresa, max(dias_mora_max) dias_mora_empresa
    from saldos_empresas
    where rownum = 1
    group by fch_proceso, no_unico, cliente
)

select a.fecha_proceso, year(a.fecha_proceso)*100+month(a.fecha_proceso) aniomes,
    empresa_ar, 
    nvl(b.cliente,"S/I") nombre_cliente, nvl(b.referencias_empresa, 0) referencias_empresa, 
    nvl(b.saldo_empresa, 0) saldos_empresa, 
    nvl(b.dias_mora_empresa, 0) dias_mora_empresa,
    empleados,saldo_empleados,empleados_sin_mora,saldo_sin_mora,
    empleados_mora_30,saldo_mora,empleados_mora_mas30,saldo_mora_mas30
from proceso_bana_vbeyg.ca_covid19_ar_tmp2 a 
left outer join saldos_dash b on a.fecha_proceso = b.fch_proceso and a.empresa_ar = b.no_unico 
;

compute stats proceso_bana_vbeyg.ca_covid19_empleador;

drop table proceso_bana_vbeyg.ca_covid19_empleador purge;


create table proceso_bana_vbeyg.ca_covid19_empleador stored as parquet as
with  saldo_totales_dash as ( 
    select a.fch_proceso, a.no_unico, a.cliente, a.segment1, a.no_cuenta, status, a.cod_producto, a.saldo, a.dias_mora_max, 
    row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, 
    ingestion_month desc, ingestion_day desc) as rownum
    from s_bana_productos.basig_dashba_dash_portafolio_his a 
    where a.fch_proceso  >= '2019-01-01 00:00:00.0'
), saldos_dash_validados as (
    select * 
    from saldo_totales_dash
    where rownum = 1 and segment1 = '1. PORTAFOLIO PRESTAMOS' and status != 'S'
), saldos_dash as (
    select fch_proceso, no_unico, cliente, 
    count(no_cuenta) referencias_empresa, sum(saldo) saldo_empresa, max(dias_mora_max) dias_mora_empresa
    from saldos_dash_validados a join proceso_bana_vbeyg.ca_covid19_ar_tmp2 b 
    on a.fch_proceso = b.fecha_proceso and a.no_unico = empresa_ar
    --where rownum = 1
    group by fch_proceso, no_unico, cliente
)

select a.fecha_proceso,empresa_ar, empleados, 
nvl(b.cliente,"S/I") cliente, nvl(b.referencias_empresa,0) referencias_empresa, 
nvl(b.saldo_empresa,0) saldo_empresa, nvl(b.dias_mora_empresa,0) dias_mora_empresa, 
saldo_empleados,empleados_sin_mora,saldo_sin_mora,
empleados_mora_30,saldo_mora,empleados_mora_mas30,saldo_mora_mas30
from proceso_bana_vbeyg.ca_covid19_ar_tmp2 a 
left outer join saldos_dash b on a.fecha_proceso = b.fch_proceso and a.empresa_ar = b.no_unico 
;

compute stats proceso_bana_vbeyg.ca_covid19_empleador;

--unicos_empresasar as (
--    select fecha_proceso, empresa_ar
--   from proceso_bana_vbeyg.ca_covid19_ar_tmp2
--),