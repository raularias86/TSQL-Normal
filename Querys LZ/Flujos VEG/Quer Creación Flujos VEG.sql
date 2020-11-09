--------1. HACER TABLA DE FLUJOS DE CANALES ALTERNOS (ENTRADA VÍA ACH Y ENTRADAS Y SALIDAS VÍA TRANS INTL)
drop table if exists proceso_bana_vbeyg.transcciones_canales_alternos_tmp1 purge;
      
create table proceso_bana_vbeyg.transcciones_canales_alternos_tmp1 stored as parquet as 
select   to_timestamp(cast(fecha_compra as string),'yyyyMMdd') fecha_transaccion,
        'Ingreso' tipo_trx,
        'POS' nombre_canal,
        concat(a.medio_adquirencia,' - ',a.tipo_facturacion) accion_canal,
        --a.tipo_facturacion accion_canal,
        'Ingreso POS' contexto,
        a.unico_comercio_adq numero_unico,
        a.monto_compra monto
from resultados_bana_vghi.fco_facturacion a
where a.fecha_compra between 20190101 and year(now())*10000+month(now())*100+day(now())-1
union all
select  a.fch_proc fecha_transaccion, 
       'Salida' Tipo_trx,
       'ACH' nombre_canal, 
       CASE WHEN a.tranem = 'ACHJ' THEN 'Pago de Pensiones entre bancos ACH'
            WHEN a.tranem = 'ACHP' THEN 'Pago proveedores ACH'
            WHEN a.tranem = 'ACHC' THEN 'Cobro comisión trx entre bancos ACH'
            WHEN a.tranem = 'ACHT' THEN 'Transferencia entre bancos ACH'
            WHEN a.tranem = 'ACHL' THEN 'Comsión trx por lotes ACH'
        END as 
       accion_canal,
       'Salida ACH' contexto,        
        a.tracun numero_unico,
        a.traamt monto
from s_bana_productos.basig_dwba_ttran_hist_part a
where a.tranem like 'ACH%'
    and a.tracde = 'DB'
    and a.fch_proc >= '2019-01-01 00:00:00.0' 
union all 
select  a.fch_proc fecha_transaccion, 
       'Ingreso' Tipo_trx,
       'ACH' nombre_canal, 
       'Transferencia ACH' accion_canal,
       'Ingreso ACH' contexto,        
        a.tracun numero_unico,
        a.traamt monto
from s_bana_productos.basig_dwba_ttran_hist_part a
where a.tranem like 'ACH '
    and a.tracde = 'DP'
    and a.fch_proc >= '2019-01-01 00:00:00.0'
union all
select  fch_proc fecha_transaccion, 
       'Ingreso' Tipo_trx,
       'SWIFT' nombre_canal, 
       'Transferencia Intl' accion_canal,
       'Entrada Transferencia Intl' contexto,
        a.tracun numero_unico,
        case when a.traamt < a.tranet then a.tranet else a.traamt end as monto
from s_bana_productos.basig_dwba_ttran_hist_part a
WHERE TRACDE = 'WD' --ORDER BY TRANEM;
    and a.fch_proc >= '2019-01-01 00:00:00.0'
union ALL
select  fch_proc fecha_transaccion, 
        'Salida' Tipo_trx,
       'SWIFT' nombre_canal, 
       'Transferencia Intl' accion_canal,
       'Salida Transferencia Intl' contexto,
        a.tracun numero_unico,
        case when a.traamt < a.tranet then a.tranet else a.traamt end as monto
from s_bana_productos.basig_dwba_ttran_hist_part a
WHERE TRACDE = 'WC' --ORDER BY TRANEM;
    and fch_proc >= '2019-01-01 00:00:00.0'
;

compute stats proceso_bana_vbeyg.transcciones_canales_alternos_tmp1;

--------2. HACER TABLA DE FLUJOS DE LBTR
drop table if exists proceso_bana_vbeyg.transcciones_canales_alternos_tmp2 purge;

create table proceso_bana_vbeyg.transcciones_canales_alternos_tmp2 stored as parquet as
with out_lbtr as (
    select *, 'Nota de Cargo BCR' accion_canal--distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2019-01-01 00:00:00.0' 
        and tracde = 'DB'
        and tranem like 'ND%'
        and trabth = 2308
    union all 
    select *, 'Traslado Automático BCR' accion_canal--distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2019-01-01 00:00:00.0' 
        and tracde = 'DB'
        and tranem like 'TBCR%'
        and trabth = 1402
), lbtr_salida as (
select  a.fch_proc fecha_transaccion, 
        'Salida' tipo_trx,
        'LBTR' canal, 
        a.accion_canal, 
        'Salida vía LBTR' contexto,
        a.tracun numero_unico, --b.capcodcliente, 
        a.traamt monto
        --travdm, travdy, tracde, tranem, sum(traamt) monto, count(1) trxs
from out_lbtr a 
), 
lbtr_entrada as (
    select 'Pago de Ministerio' accion_canal, *--distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2019-01-01 00:00:00.0' 
        and tracde = 'CR'
        and tranem like 'PPRV%'
        and trabth = 1416
        and traacc !=0
    union all
    select 'Nota de Abono LBTR' accion_canal, * --distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2019-01-01 00:00:00.0' 
        and tracde = 'CR'
        and tranem like 'LBTR%'
        and trabth = 1401
        and traacc != 0
    union all
    select 'Nota de Abono Manual LBTR' accion_canal, * --distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2019-01-01 00:00:00.0'
        and tracde = 'CR'
        and tranem like 'NA%'
        and trabth in (2311, 2309)
        and traacc != 0
)
select  a.fch_proc fecha_transaccion, 
        'Ingreso' tipo_trx,
        'LBTR' canal, 
        a.accion_canal, 
        'Entrada vía LBTR' contexto,
        a.tracun numero_unico,  
        a.traamt monto
from lbtr_entrada a 
union all
select * 
from lbtr_salida
;

compute stats proceso_bana_vbeyg.transcciones_canales_alternos_tmp2;


----3. UNIR CON TABLA DE FLUJOS DE LA HUELLA TRX
drop table if exists proceso_bana_vbeyg.flujos_veg_tmp purge;

create table proceso_bana_vbeyg.flujos_veg_tmp stored as parquet as
with all_trxs as (
    --Agregando lo de facturación, ach ingreso, transferencias intl
select fecha_transaccion,
        tipo_trx,
        nombre_canal,
        accion_canal,
        contexto,
        numero_unico,
        monto
from proceso_bana_vbeyg.transcciones_canales_alternos_tmp1
where fecha_transaccion >= '2019-01-01 00:00:00.0'
union all 
    --Agregando lo de lbtr, entrada y salida
select  fecha_transaccion,
        tipo_trx,
        canal nombre_canal,
        accion_canal,
        contexto, 
        numero_unico,
        monto
from proceso_bana_vbeyg.transcciones_canales_alternos_tmp2
union all
select to_timestamp(cast(sk_fecha_transaccion as string),'yyyyMMdd') fecha_transaccion,
       'Ingreso' Tipo_trx,
       a.nombre_canal, 
       a.accion_canal,
       a.contexto,
       agrupacion_sk_cliente_destino numero_unico,
       monto
from    proceso_bana.veg_huella_trx a
where   sk_fecha_transaccion between 20190101 and year(now())*10000+month(now())*100+day(now())-1
and     a.accion_canal != 'LiquidacióN FacturacióN Pos'
union all
select to_timestamp(cast(sk_fecha_transaccion as string),'yyyyMMdd') fecha_transaccion,
       'Salida' Tipo_trx,
       a.nombre_canal, 
       a.accion_canal,
       a.contexto,
       agrupacion_sk_cliente_origen numero_unico,
       monto
from proceso_bana.veg_huella_trx a
where sk_fecha_transaccion between 20190101 and year(now())*10000+month(now())*100+day(now())-1
and a.accion_canal not like '%Ach%'
),
cifgen as (
select
        cifcodcliente as no_unico,
        cifnombreclie as nombre_cliente,
        lpad(cast(cifcodactivid as string),6,"0") as codciiu,
        cifcodejecuti,
        cifcodareafin
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
        b.cifcodareafin,
        c.findesareafin area_financiera,
        INITCAP(SECBA.FINSECTORBANCA) segmentobanca, 
        INITCAP(SECBA.FINSUBGRUPO) SubSegmentoBanca,
        a.nombre_canal, 
        a.accion_canal,
        a.contexto,
        Tipo_trx,
        count(1) trxs,
        sum(monto) monto
from all_trxs a
left join cifgen b on a.numero_unico =b.no_unico
left join catareafinanc c on b.cifcodareafin=c.FINCODAREAFIN
LEFT JOIN catsectorbanca SECBA ON SECBA.FINCODSECTORBCA=c.FINCODSECTORBANCA
--left join fechas_fin_mes d on a.
--WHERE day(fecha_transaccion) between 1 and day(now())
where 
--fecha_transaccion >= '2020-05-01 00:00:00.0'
--and fecha_transaccion <= '2020-05-31 00:00:00.0'
b.cifcodareafin in --(1103,1106,1113,1112,1111,1120,1123,1101)
 (1103,1106,1113,1112,1111,1120,1123,1101,1241,1242,1243,1230,1244)
--and a.numero_unico = 21898
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
) --order by 10,8 desc
, sectores_veg as (
    select  a.no_unico, 
            a.no_master, 
            a.grupo_economico,
            a.tipo_cartera,
            a.sector_ba, 
            a.sub_sector_ba, 
            a.actividad_ba, 
            a.cluster_ba,
            a.nivel_riesgo,
            a.escenario_recuperacion
    from proceso_bana_vbeyg.clientes_veg_mensual a  
            --join cifgen b on a.no_master = b.no_unico
            --left outer join resultados_bana_vbeyg.catalogo_sectorial_ba c 
            --on lpad(cast(a.cifcodactivid as string),6,"0")  = c.cod_portal
            --where c.fch_catalogo = '2020-09-30 00:00:00'
)
select  b.fecha_max_mes, a.*,
        nvl(c.no_master, a.numero_unico) no_master,
        nvl(c.grupo_economico, a.nombre_cliente) nombre_grupo,
        nvl(c.tipo_cartera,"Asignado") tipo_cartera,
        nvl(c.sector_ba,"ND") sector_ba,
        nvl(c.sub_sector_ba,"ND") sub_sector_ba,
        nvl(c.actividad_ba, "ND") actividad,
        nvl(c.cluster_ba,"ND") cluster,
        nvl(c.nivel_riesgo,"ND") nivel_impacto,
        nvl(c.escenario_recuperacion,"S/D") escenario_recuperacion
from detalle_trx a 
join fechas_fin_mes b on a.fecha_mes = b.fecha_mes
left outer join sectores_veg c on a.numero_unico = c.no_unico
where a.cifcodareafin in (1103,1106,1113,1112,1111,1120,1123,1101)
union all 
select  b.fecha_max_mes, a.*,
        a.numero_unico  no_master, 
        a.nombre_cliente nombre_grupo,
        "Asignado" tipo_cartera,
        nvl(c.sector_ba,"ND") sector_ba,
        nvl(c.sub_sector_ba,"ND") sub_sector_ba,
        nvl(c.actividad_ba, "ND") actividad,
        nvl(c.cluster_ba,"ND") cluster,
        nvl(c.nivel_riesgo,"ND") nivel_impacto,
        nvl(c.escenario_recuperacion,"S/D") escenario_recuperacion
from detalle_trx a 
join fechas_fin_mes b on a.fecha_mes = b.fecha_mes
left join resultados_bana_vbeyg.catalogo_sectorial_ba c on a.codciiu = c.cod_portal
where  a.cifcodareafin in (1241,1242,1243,1230,1244)
AND c.fch_catalogo = '2020-09-30 00:00:00.0'
--order by 1,2,3,4,5,6,7,8,9,10,11,12
;

compute stats  proceso_bana_vbeyg.flujos_veg_tmp;

-------borrar las tablas temporales
drop table if exists proceso_bana_vbeyg.transcciones_canales_alternos_tmp1 purge;
drop table if exists proceso_bana_vbeyg.transcciones_canales_alternos_tmp2 purge;

---para agregar el comparativo de los saldos a los flujos
drop table if exists proceso_bana_vbeyg.flujos_veg_tmp2 purge;

create table proceso_bana_vbeyg.flujos_veg_tmp2 stored as paquet as 
with saldos_fin_mes as (
    select a.aniomes, a.no_unico, 
    case when a.segment1 = '1. PORTAFOLIO PRESTAMOS' then 'PRESTAMOS' ELSE 'DEPOSITOS' END AS PRODUCTO, 
    sum(a.saldo) saldo_contable, sum(a.saldo_promedio) saldo_promedio
    from proceso_bana_vbeyg.reciprocidad_mensual a 
    group by 1,2,3
), saldos_activos as (
    select aniomes, no_unico, saldo_contable saldo_prestamos, saldo_promedio promedio_prestamos
    from saldos_fin_mes
    where producto = 'PRESTAMOS'
), saldos_depositos as (
    select aniomes, no_unico, saldo_contable  saldo_depositos, saldo_promedio promedio_depositos
    from saldos_fin_mes
    where producto = 'DEPOSITOS'
), unicos_x_mes as (
    select distinct aniomes, no_unico
    from saldos_fin_mes
), saldos_pivote as (
    select a.aniomes, a.no_unico, nvl(b.saldo_prestamos,0) saldo_prestamos, nvl(b.promedio_prestamos,0) promedio_prestamos,
    nvl(c.saldo_depositos,0) saldo_depositos, nvl(c.promedio_depositos, 0) promedio_depositos
    from unicos_x_mes a 
    left outer join saldos_activos b on a.aniomes = b.aniomes and a.no_unico = b.no_unico
    left outer join saldos_depositos c on a.aniomes = c.aniomes and a.no_unico = c.no_unico
    limit 100;
)

select *
from proceso_bana_vbeyg.flujos_veg_tmp a
--left outer join saldos_pivote b 
--on a.aniomes = b.fecha_mes and a.numero_unico = b.no_unico 
limit 100;

;


