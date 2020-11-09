
drop table if exists proceso_bana_vbeyg.saldos_clientes_ssf_tmp1 purge;

create table proceso_bana_vbeyg.saldos_clientes_ssf_tmp1 stored as parquet as 
with clientes_ssf as (
    select  nit,
            nombre_deudor,
            fecha_corte,
            codigo,
            institucion,
            tipo_institucion,
            cod_destino_bcr,
            descripcion_destino_bcr,
            tipo_cartera,
            tipo_financiamiento,
            no_referencia,
            clasificacion_riesgo,
            monto_otorgado,
            saldo_adeudado,
            saldo_vencido_capital,
            saldo_vigente_interes,
            saldo_vencido_interes,
            saldo_vigente_capital,
            dias_mora_capital,
            saldo_mora_capital,
            dias_mora_interes,
            saldo_mora_interes,
            tipo_prestamo,
            total_riesgo,
            fecha_otorgado,
            fecha_vencimiento,
            fecha_castigo, 
            estado,
            row_number() over (partition by a.nit, a.no_referencia, a.estado, a.tipo_institucion, a.codigo, a.tipo_cartera, a.tipo_financiamiento,
            a.fecha_otorgado, a.fecha_corte order by a.fecha_corte desc) as rownum 
    from  proceso_bana_vbeyg.tabla_saldos_ssf_clientes_2020 a
), clientes_ssf_validados as (
    select * --fecha_corte, count(1)
    from clientes_ssf
    where rownum = 1
), info_cifgenerales as (
    select  ltrim(rtrim(a.cifnit)) nit, a.cifcodcliente no_unico, a.cifnombreclie nombre_cliente_ba, 
            a.cifcodareafin cod_area_financiera, 
            b.findesareafin area_financiera, 
            --b.vicepresidencia,
            case when a.cifcodareafin in (1101,1103,1106,1111,1112,1113,1120,1123) then 'VEG'
                 when a.cifcodareafin in (1230,1241,1242,1243,1244) then 'VPP(PYME)'
                 when a.cifcodareafin in (1121,1199,1200,1214,1510,1518,1519,1520,1521,1524) then 'VPP'
                 when a.cifcodareafin in (1436,9000,9999,9999,9002) then 'NO EXISTE'
                 else 'NA' end as vicepresidencia,
            a.cifcodejecuti cod_ejecutivo, 
            a.ciftipoclient tipo_cliente
    from  s_bana_clientes.riesgodb_dwba_cifgenerales a
    left join s_bana_productos.basig_bacatalog_catareafinanc b on a.cifcodareafin = b.fincodareafin
    where a.ciffechaproceso = '2020-10-31 00:00:00.0'
        and b.ingestion_year=2020
        and b.ingestion_month=3
        and b.ingestion_day=1
), agrupador_sector_ssf as (
    select  distinct case when a.cod_destino_bcr = '89100' then '10. Servicios  ' 
                when a.cod_destino_bcr is null then 'Sin Determinar'
                when a.cod_destino_bcr = '' then 'Sin Determinar'
                else b.desc_agrupador_destino end as agrupador_sector_Destino,
            case when b.cod_destino_bcr in ('11. Consumo Familiar') then 'CONSUMO' 
                when b.cod_destino_bcr in ('13. Vivienda') then 'VIVIENDA'
                else 'PRODUCTIVO' 
            END as tipo_sector_destino_bcr,
            a.cod_destino_bcr
    from  clientes_ssf_validados a 
    left outer join proceso_bana_vbeyg.catalogo_destinos_bcrssf b 
    on a.cod_destino_bcr = b.cod_destino_bcr

)
select fecha_corte, a.nit, nombre_deudor, nvl(b.no_unico,0) no_unico, 
    nvl(b.nombre_cliente_ba, 'No Cliente BA') nombre_cliente_ba, 
    b.vicepresidencia, b.cod_area_financiera, b.area_financiera, 
    b.cod_ejecutivo, b.tipo_cliente,
    nvl(c.tipo_cartera, 'ND') tipo_cartera_asignada, nvl(c.sector_ba,'ND') sector_ba,
    nvl(c.sub_sector_ba,'ND') sub_sector_ba, nvl(c.actividad_ba, 'ND') actividad_ba, nvl(c.cluster_ba,'ND') cluster_ba,
    codigo,institucion,tipo_institucion,a.cod_destino_bcr,a.descripcion_destino_bcr,
    d.agrupador_sector_Destino, d.tipo_sector_destino_bcr,
    a.tipo_cartera,tipo_financiamiento,no_referencia,clasificacion_riesgo,monto_otorgado,saldo_adeudado,
    saldo_vencido_capital,saldo_vigente_interes,saldo_vencido_interes,saldo_vigente_capital,dias_mora_capital,
    saldo_mora_capital,dias_mora_interes,saldo_mora_interes,
    tipo_prestamo,total_riesgo,fecha_otorgado,fecha_vencimiento,fecha_castigo,estado
from clientes_ssf_validados a 
left outer join info_cifgenerales b
on cast(a.nit as string) = cast(b.nit as string) 
left outer join clientes_veg_mensual c 
on b.no_unico = c.no_unico 
left outer join agrupador_sector_ssf d
on a.cod_destino_bcr = d.cod_destino_bcr
order by a.fecha_corte, b.no_unico, a.nit, a.institucion
;

compute stats proceso_bana_vbeyg.saldos_clientes_ssf_tmp1;

-----------------------------------------------------------------------------------------------------
-----------------------cálculo de tasas y flag de prorroga COVID BA----------------------------------
-----------------------------------------------------------------------------------------------------

drop table proceso_bana_vbeyg.saldos_clientes_ssf purge;

create table proceso_bana_vbeyg.saldos_clientes_ssf stored as parquet as 
with tasa_anual as (
    select a.fecha_corte,
           a.nit, 
           a.tipo_cartera, 
           a.tipo_financiamiento, 
           a.no_referencia, 
           a.estado,
           a.institucion,
           a.monto_otorgado, 
           a.saldo_adeudado,
           a.saldo_vigente_interes/a.saldo_adeudado as interes_mensual, 
           power((a.saldo_vigente_interes/a.saldo_adeudado)+1,12)-1 as tasa_anual, 
           cast(strright(a.fecha_corte,2) as int) as dia_corte,
           cast(strright(a.fecha_otorgado,2) as int) as dia_otorgado
    from saldos_clientes_ssf_tmp1 a
    where a.tipo_cartera = '01 Cartera Propia'
    and a.tipo_financiamiento = 'PD Prestamos'
    and a.saldo_adeudado > 0
    and a.tipo_prestamo in ('CD','CR','NR')
    and a.estado = 'Vigente'
), tasa_efectiva as (
    select  a.*, 
            (a.tasa_anual/
            (case when (a.dia_corte - a.dia_otorgado)+1 <= 0 
                then 1 
                else (a.dia_corte - a.dia_otorgado)+1 end))*30 as tasa_efectiva
    from tasa_anual a
    --where fecha_corte = '2020-04-30'
), detalle_covid as (
    select distinct a.no_unico,
            'Cliente CO BA' flag_prorroga_co
    from proceso_bana.ca_covid_alivios a 
)
    select a.*, 
            b.interes_mensual, 
            b.tasa_anual, 
            b.tasa_efectiva, 
            case when b.tasa_efectiva is null then 'Sin Estimación Tasa'
                when b.tasa_efectiva < 0.03 or b.tasa_efectiva > 0.25 then 'Estimación de Tasa no Válida'
                else 'Estimación de Tasa Válida' end as flag_estimacion_tasa,
            nvl(c.flag_prorroga_co,'Sin Prorroga CO') flag_prorroga_co
    from saldos_clientes_ssf_tmp1 a left outer join tasa_efectiva b 
    on a.fecha_corte = b.fecha_corte and a.nit = b.nit and a.tipo_cartera = b.tipo_cartera 
    and a.tipo_financiamiento = b.tipo_financiamiento and a.no_referencia = b.no_referencia 
    and a.estado = b.estado and a.institucion = b.institucion and a.monto_otorgado = b.monto_otorgado 
    and a.saldo_adeudado = b.saldo_adeudado 
    left outer join detalle_covid c on a.no_unico = c.no_unico 
;

COMPUTE STATS  proceso_bana_vbeyg.saldos_clientes_ssf;