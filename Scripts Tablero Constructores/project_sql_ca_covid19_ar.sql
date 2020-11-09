--tabla con el detalle por único del empleado, asociado a su empleador (empresa_ar)
drop table if exists proceso_bana_vghi.ca_covid19_ar_tmp  purge;

--primero se trae el detalle de los código de AR y único, exceptuando los indicados más abajo
create table proceso_bana_vghi.ca_covid19_ar_tmp stored as parquet as 
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
       sum(colsaldo) saldo 
from s_bana_productos.riesgodb_dwba_coldocum a
inner join agrmst b on a.colagente=b.agrcod
where colfchproc >= '2019-01-01 00:00:00.0'
  and trim(colagente) is not null
  and cast(colagente as int) not in (0,1,8000,10014) --1 = BA, 8000 = SIN AR, 10014 = CON OPI SIN AGENTE DE RETENCION
  and colsaldo>0
  and colcodtiposaldo<>3
group by 1,2,3
;
compute stats proceso_bana_vghi.ca_covid19_ar_tmp ;



drop table if exists proceso_bana_vghi.ca_covid19_ar_tmp2  purge;

create table proceso_bana_vghi.ca_covid19_ar_tmp2 stored as parquet as 
with
dash_ba as
(
    select
        no_unico,
        fch_proceso,
        no_cuenta,
        saldo,
        monto,
        status,
        segment1,
        row_number() over (partition by no_unico, no_cuenta, cod_producto, fch_proceso order by ingestion_year desc, ingestion_month desc, ingestion_day desc) as rownum 
    from s_bana_productos.basig_dashba_dash_portafolio_his
    where
        fch_proceso >= '2019-01-01 00:00:00.0'
),
dash_definitivo as
(
    select 
        * 
    from dash_ba 
    where 
        rownum = 1
        and segment1 = '1. PORTAFOLIO PRESTAMOS'
),
activos_diarios as
(
    select
        no_unico,
        fch_proceso,
        sum(saldo) as activos
    from dash_definitivo as a
    group by
        no_unico,
        fch_proceso
),
empresas_ar as (
select a.*,
       row_number() over (partition by a.fecha_proceso,a.no_unico order by a.saldo desc) rn
from proceso_bana_vghi.ca_covid19_ar_tmp a
left anti join proceso_bana_vghi.ca_covid19_riesgo_planilla_mensual_activos b 
on (year(a.fecha_proceso)*100+month(a.fecha_proceso))=b.fecha_mes and a.no_unico=b.no_unico
), 
empresas_ar_filtered as (
select * 
from empresas_ar
where rn=1
)
select
    a.no_unico,
    a.fecha_proceso,
    a.empresa_ar,
    nvl(b.activos, 0) as activos
from empresas_ar_filtered as a
left join activos_diarios as b
on
    a.no_unico = b.no_unico and a.fecha_proceso = b.fch_proceso
;
compute stats proceso_bana_vghi.ca_covid19_ar_tmp2;



drop table if exists proceso_bana_vghi.ca_covid19_ar  purge;

create table proceso_bana_vghi.ca_covid19_ar stored as parquet as 
select
    empresa_ar,
    year(fecha_proceso)*100+month(fecha_proceso) fecha_mes,
    count(*) as empleados_mes,
    sum(activos) as activos_empleados_mes
from  proceso_bana_vghi.ca_covid19_ar_tmp2
group by 1,2
;
compute stats proceso_bana_vghi.ca_covid19_ar;
