drop table if exists proceso_bana_vghi.ca_covid19_huella_tmp purge;

create table proceso_bana_vghi.ca_covid19_huella_tmp stored as parquet as
with grls as (
select a.cifcodcliente cod_empresa,
          a.CIFNOMBRECLIE NOM_EMPRESA
from s_bana_clientes.riesgodb_dwba_cifgenerales a
where ciffechaproceso = '2020-04-30 00:00:00.0'
),
masters as (
select a.cod_empresa,
          a.nom_empresa,
          m1.cod_master master,
          m1.grupo_economico nombre_master,
         COALESCE(M5.COD_MASTER, M4.COD_MASTER, M3.COD_MASTER, M2.COD_MASTER, M1.COD_MASTER) AS master_grupo,
         COALESCE(M5.grupo_economico, M4.grupo_economico, M3.grupo_economico, M2.grupo_economico, M1.grupo_economico) AS nombre_master_grupo
from grls a
left join proceso_bana_vghi.universo_privado_huella m1 on a.cod_empresa=m1.no_unico
left join proceso_bana_vghi.universo_privado_huella m2 on m1.cod_master=m2.no_unico and m2.cod_master<>m1.cod_master and m2.cod_master<>0
left join proceso_bana_vghi.universo_privado_huella m3 on m3.no_unico=m2.cod_master and m2.cod_master<>m3.cod_master and m3.cod_master<>0
left join proceso_bana_vghi.universo_privado_huella m4 on m3.cod_master=m4.no_unico and m4.cod_master<>m3.cod_master and m4.cod_master<>0
left join proceso_bana_vghi.universo_privado_huella m5 on m5.no_unico=m4.cod_master and m4.cod_master<>m5.cod_master and m5.cod_master<>0
),
huella as (
select sk_fecha_transaccion,
       agrupacion_sk_cliente_origen cliente_origen,
       agrupacion_sk_cliente_destino cliente_destino,
       ori.nom_empresa nombre_cliente_origen,
       dest.nom_empresa nombre_cliente_destino,
       case when nvl(ori.master,agrupacion_sk_cliente_origen)=nvl(dest.master,agrupacion_sk_cliente_destino) then 1 else 0 end flag_mismo_master,
       case when nvl(ori.master_grupo,agrupacion_sk_cliente_origen)=nvl(dest.master_grupo,agrupacion_sk_cliente_destino) then 1 else 0 end flag_mismo_master_grupo,
       sum(monto) monto
from proceso_bana_vghi.veg_huella_trx a
left join masters ori on a.agrupacion_sk_cliente_origen=ori.cod_empresa
left join masters dest on a.agrupacion_sk_cliente_destino=dest.cod_empresa
where sk_fecha_transaccion >=20181201
group by 1,2,3,4,5,6,7
)
select sk_fecha_transaccion, 
          cliente_origen,
          case when cliente_origen=1111111111 then 'Entrada SF' 
               else nombre_cliente_origen 
          end nombre_cliente_origen,
          cliente_destino,
          nombre_cliente_destino,
          flag_mismo_master,
          flag_mismo_master_grupo,
          concat(cast(cliente_origen as string),cast(cliente_destino as string)) joinkey,
          monto
from huella
where cliente_origen<>cliente_destino
   and cliente_destino not in (2222222222,4444444444,8888888888,3333333333)
;

compute stats proceso_bana_vghi.ca_covid19_huella_tmp;


--añadiendo fechas que faltan el las relaciones

drop table if exists proceso_bana_vghi.ca_covid19_huella_tmp2 purge;

create table proceso_bana_vghi.ca_covid19_huella_tmp2 stored as parquet as
with relaciones as (
select cliente_origen,
          nombre_cliente_origen,
          cliente_destino,
          nombre_cliente_destino,
          flag_mismo_master,
          flag_mismo_master_grupo,
          joinkey
from proceso_bana_vghi.ca_covid19_huella_tmp
group by 1,2,3,4,5,6,7
),
meses as (
select (sk_fecha_transaccion div 100) fecha_mes
from proceso_bana_vghi.ca_covid19_huella_tmp
group by 1
),
vector_relaciones as (
select fecha_mes,
       cliente_origen,
       nombre_cliente_origen,
       cliente_destino,
       nombre_cliente_destino,
       flag_mismo_master,
       flag_mismo_master_grupo,
       joinkey
from meses a left join relaciones b on 1=1
)
select  cast(nvl(cast(b.sk_fecha_transaccion as string),concat(cast(fecha_mes as string),'01')) as int) sk_fecha_transaccion,
           nvl(b.cliente_origen,a.cliente_origen) cliente_origen,
           nvl(b.nombre_cliente_origen,a.nombre_cliente_origen) nombre_cliente_origen,
           nvl(b.cliente_destino,a.cliente_destino) cliente_destino,
           nvl(b.nombre_cliente_destino,a.nombre_cliente_destino) nombre_cliente_destino,
           nvl(b.flag_mismo_master,a.flag_mismo_master) flag_mismo_master,
           nvl(b.flag_mismo_master_grupo,a.flag_mismo_master_grupo) flag_mismo_master_grupo,
           a.joinkey,
           nvl(b.monto,0) monto
from vector_relaciones a
left join proceso_bana_vghi.ca_covid19_huella_tmp b on a.fecha_mes=(b.sk_fecha_transaccion div 100) and a.joinkey=b.joinkey;

compute stats  proceso_bana_vghi.ca_covid19_huella_tmp2;



drop table if exists proceso_bana_vghi.ca_covid19_huella_tmp3 purge;

create table proceso_bana_vghi.ca_covid19_huella_tmp3 stored as parquet as
with vs_mant as ( 
select year(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),1))*100+ month(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),1)) fecha_mes_join,
           (sk_fecha_transaccion div 100) fecha_mes,
           joinkey,
           sum(case when 
                YEAR(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),1))=2020 
                AND month(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),1))=5
                and day(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),1))>=day(now())
               then 0 
               else monto 
            end) monto
from  proceso_bana_vghi.ca_covid19_huella_tmp2
where (sk_fecha_transaccion div 100) < 202005
group by 1,2,3
),
vs_yant  as (
select year(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),12))*100+ month(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),12)) fecha_mes_join,
           (sk_fecha_transaccion div 100) fecha_mes,
           joinkey,
           sum(case when 
                YEAR(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),1))=2020 
                AND month(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),1))=5
                and day(add_months(to_timestamp(cast(sk_fecha_transaccion as string), 'yyyyMMdd'),1))>=day(now())
               then 0 
               else monto 
            end) monto
from  proceso_bana_vghi.ca_covid19_huella_tmp2
where (sk_fecha_transaccion div 100) < 201906
group by 1,2,3
),
flujos as (
select a.*,
         nvl(mant.monto,0) mes_anterior,
         nvl(yant.monto,0) anio_anterior,
         row_number() over (partition by a.joinkey, (sk_fecha_transaccion div 100) order by sk_fecha_transaccion desc) rn
from  proceso_bana_vghi.ca_covid19_huella_tmp2 a
left join vs_mant mant on a.joinkey=mant.joinkey and (a.sk_fecha_transaccion div 100)=mant.fecha_mes_join
left join vs_yant yant on a.joinkey=yant.joinkey and (a.sk_fecha_transaccion div 100)=yant.fecha_mes_join
where sk_fecha_transaccion>=20190101
)
select (sk_fecha_transaccion div 100) fecha_mes,
        sk_fecha_transaccion,
          cliente_origen,
          nombre_cliente_origen,
          cliente_destino,
          nombre_cliente_destino,
          flag_mismo_master,
          flag_mismo_master_grupo,
          joinkey,
          monto,
          case when rn=1 then mes_anterior else 0 end monto_mes_anterior,
          case when rn=1 then anio_anterior else 0 end monto_anio_anterior
from flujos;

compute stats proceso_bana_vghi.ca_covid19_huella_tmp3;


drop table if exists proceso_bana_vghi.ca_covid19_huella purge;

create table proceso_bana_vghi.ca_covid19_huella stored as parquet as
with huella as (

select fecha_mes,
--        sk_fecha_transaccion,
        cliente_origen,
        nombre_cliente_origen,
        cliente_destino,
        nombre_cliente_destino,
        flag_mismo_master,
        flag_mismo_master_grupo,
        joinkey,
        sum(monto) monto,
        sum(monto_mes_anterior) monto_mes_anterior,
        sum(monto_anio_anterior) monto_anio_anterior
from proceso_bana_vghi.ca_covid19_riesgo_huella_tmp3
group by 1,2,3,4,5,6,7,8
),
huella_agru as (
select *
from huella
where (monto+monto_mes_anterior+monto_anio_anterior)>0
)
select a.fecha_mes,
--        a.sk_fecha_transaccion,
        a.cliente_origen,
        a.nombre_cliente_origen,
        nvl(b.master,a.cliente_origen) master_origen,
        nvl(b.nombre_master,a.nombre_cliente_origen) nombre_master_origen,
        nvl(b.master_grupo,a.cliente_origen) master_grupo_origen,
        nvl(b.nombre_master_grupo,a.nombre_cliente_origen) nombre_master_grupo_origen,
        nvl(b.tamano_comercial,0) tamano_comercial_origen,
        nvl(b.activos_empresa,0) activos_empresa,
        nvl(b.cantidad_empleados,0) cantidad_empleados,
        nvl(b.activos_empleados,0) activos_empleados,
        a.cliente_destino,
        a.nombre_cliente_destino,
        a.flag_mismo_master,
        a.flag_mismo_master_grupo,
        concat(cast(a.cliente_destino as string),cast(a.fecha_mes as string)) joinkey,
        a.monto,
        a.monto_mes_anterior,
        a.monto_anio_anterior
from huella_agru a
left join proceso_bana_vghi.ca_covid19_riesgo_cliente b on a.cliente_origen=b.cod_empresa;

compute stats proceso_bana_vghi.ca_covid19_huella;
