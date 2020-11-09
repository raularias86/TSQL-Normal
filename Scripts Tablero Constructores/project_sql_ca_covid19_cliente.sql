drop table if exists proceso_bana_vghi.ca_covid19_cliente_tmp purge;

create table proceso_bana_vghi.ca_covid19_cliente_tmp stored as parquet as
with grls as (
select a.cifcodcliente cod_empresa,
          a.CIFNOMBRECLIE NOM_EMPRESA,
          a.cifcodareafin,
          c.findesareafin area_financiera,
          iNITCAP(SECBA.FINSECTORBANCA) segmentobanca, 
          INITCAP(SECBA.FINSUBGRUPO) SubSegmentoBanca
from s_bana_clientes.riesgodb_dwba_cifgenerales a
left join proceso_bana_vghi.catareafinanc c on a.cifcodareafin=c.FINCODAREAFIN
LEFT JOIN PROCESO_BANA_VGHI.catsectorbanca SECBA ON SECBA.FINCODSECTORBCA=c.FINCODSECTORBANCA
where ciffechaproceso = '2020-04-30 00:00:00.0'
),
masters as (
select a.cod_empresa,
          a.nom_empresa,
          m1.cod_master master,
          m1.grupo_economico nombre_master,
          m1.tipo_institucion,
         COALESCE(M5.COD_MASTER, M4.COD_MASTER, M3.COD_MASTER, M2.COD_MASTER, M1.COD_MASTER) AS master_grupo,
         COALESCE(M5.grupo_economico, M4.grupo_economico, M3.grupo_economico, M2.grupo_economico, M1.grupo_economico) AS nombre_master_grupo
from grls a
INNER join proceso_bana_vghi.universo_privado_huella m1 on a.cod_empresa=m1.no_unico
left join proceso_bana_vghi.universo_privado_huella m2 on m1.cod_master=m2.no_unico and m2.cod_master<>m1.cod_master and m2.cod_master<>0
left join proceso_bana_vghi.universo_privado_huella m3 on m3.no_unico=m2.cod_master and m2.cod_master<>m3.cod_master and m3.cod_master<>0
left join proceso_bana_vghi.universo_privado_huella m4 on m3.cod_master=m4.no_unico and m4.cod_master<>m3.cod_master and m4.cod_master<>0
left join proceso_bana_vghi.universo_privado_huella m5 on m5.no_unico=m4.cod_master and m4.cod_master<>m5.cod_master and m5.cod_master<>0
)
SELECT a.cod_empresa,
            a.nom_empresa,
            emp.cifcodareafin areafin_emp,
            emp.area_financiera nom_areafin_emp,
            emp.segmentobanca segmento_emp,
            emp.SubSegmentoBanca subsegmento_emp,
            a.tipo_institucion,
            a.master,
            a.nombre_master,
            mst.cifcodareafin areafin_master,
            mst.area_financiera nom_areafin_master,
            mst.segmentobanca segmento_master,
            mst.SubSegmentoBanca subsegmento_master,
            a.master_grupo,
            a.nombre_master_grupo,
            grp.cifcodareafin areafin_master_grupo,
            grp.area_financiera nom_areafin_master_grupo,
            grp.segmentobanca segmento_master_grupo,
            grp.SubSegmentoBanca subsegmento_master_grupo
FROM MASTERS a
left join grls emp on a.cod_empresa=emp.cod_empresa
left join grls mst on a.master=mst.cod_empresa
left join grls grp on a.master_grupo=grp.cod_empresa
;

compute stats proceso_bana_vghi.ca_covid19_cliente_tmp;



drop table if exists proceso_bana_vghi.ca_covid19_cliente_tmp2 purge;

create table proceso_bana_vghi.ca_covid19_cliente_tmp2 stored as parquet as
with grls as (
select a.cifcodcliente cod_empresa,
          a.CIFNOMBRECLIE NOM_EMPRESA,
          a.cifcodareafin,
          c.findesareafin area_financiera,
          iNITCAP(SECBA.FINSECTORBANCA) segmentobanca, 
          INITCAP(SECBA.FINSUBGRUPO) SubSegmentoBanca
from s_bana_clientes.riesgodb_dwba_cifgenerales a
left join proceso_bana_vghi.catareafinanc c on a.cifcodareafin=c.FINCODAREAFIN
LEFT JOIN PROCESO_BANA_VGHI.catsectorbanca SECBA ON SECBA.FINCODSECTORBCA=c.FINCODSECTORBANCA
where ciffechaproceso = '2020-04-30 00:00:00.0'
)
select a.no_unico cod_empresa,
          a.nombre_completo nombre_empresa,
          b.cifcodareafin,
          b.area_financiera nom_areafin_emp,
          b.segmentobanca,
          b.SubSegmentoBanca,
          'PYME' tipo_institucion,
          a.no_unico master,
            a.nombre_completo nombre_master,
            b.cifcodareafin areafin_master,
            b.area_financiera nom_areafin_master,
            b.segmentobanca segmento_master,
            b.SubSegmentoBanca subsegmento_master,
            a.no_unico master_grupo,
            a.nombre_completo nombre_master_grupo,
            b.cifcodareafin areafin_master_grupo,
            b.area_financiera nom_areafin_master_grupo,
            b.segmentobanca segmento_master_grupo,
            b.SubSegmentoBanca subsegmento_master_grupo
from proceso_bana_vghi.fco_cliente a
left join grls b on a.no_unico=b.cod_empresa
where fecha_mes = 202004
  and nsegmento='PYME';

compute stats proceso_bana_vghi.ca_covid19_cliente_tmp2;



drop table if exists proceso_bana_vghi.ca_covid19_cliente purge;

create table proceso_bana_vghi.ca_covid19_cliente stored as parquet as 
with
clientes as
(
    select
        no_unico,
        nombre_completo,
        empresa_empleadora,
        saldo_prestamos_total + saldo_tdc_total as activos
    from proceso_bana_vghi.fco_cliente
    where
        fecha_mes = 202004
),
empresas as
(
select
    a.empresa_empleadora as no_unico_empresa,
    b.nombre_completo as nombre_empresa,
    count(*) as cantidad_empleados,
    sum(a.activos) as activos_empleados
from clientes as a
left join clientes as b
on
    a.empresa_empleadora = b.no_unico
group by 1,2
),
empresas_2 as (
select
    no_unico,
    tamano_comercial,
    saldo_prestamos_total + saldo_tdc_total as activos
from proceso_bana_vghi.fco_cliente
where fecha_mes = 202004 
),
empresas_pymes as (
select * from proceso_bana_vghi.ca_covid19_cliente_tmp
union all
select * from proceso_bana_vghi.ca_covid19_cliente_tmp2
),
distinct_emp_pym as (
select cod_empresa,
        nom_empresa,
        areafin_emp,
        nom_areafin_emp,
        segmento_emp,
        subsegmento_emp,
        tipo_institucion,
        master,
        nombre_master,
        areafin_master,
        nom_areafin_master,
        segmento_master,
        subsegmento_master,
        master_grupo,
        nombre_master_grupo,
        areafin_master_grupo,
        nom_areafin_master_grupo,
        segmento_master_grupo,
        subsegmento_master_grupo,
        row_number() over (partition by cod_empresa order by cod_empresa desc) rn
from empresas_pymes
)
select A.cod_empresa,
       A.nom_empresa,
       A.areafin_emp,
       A.nom_areafin_emp,
       A.segmento_emp,
       A.subsegmento_emp,
       A.tipo_institucion,
       A.master,
       A.nombre_master,
       A.areafin_master,
       A.nom_areafin_master,
       A.segmento_master,
       A.subsegmento_master,
       A.master_grupo,
       A.nombre_master_grupo,
       A.areafin_master_grupo,
       A.nom_areafin_master_grupo,
       A.segmento_master_grupo,
       A.subsegmento_master_grupo,
       c.tamano_comercial,
       c.activos activos_empresa,
       B.cantidad_empleados,
       b.activos_empleados
from distinct_emp_pym A
LEFT JOIN empresas B ON A.cod_empresa=B.no_unico_empresa
LEFT JOIN empresas_2 c ON A.cod_empresa=c.no_unico
where rn=1;

compute stats proceso_bana_vghi.ca_covid19_cliente;