---Nuevos flujos
---TRANSFERENCIAS INTERNACIONALES
select  fch_proc fecha_transaccion, 
       'Ingreso' Tipo_trx,
       'SWIFT' nombre_canal, 
       'Transferencia Intl' accion_canal,
       'Entrada Transferencia Intl' contexto,
        b.capcodcliente numero_unico,
        case when a.traamt < a.tranet then a.tranet else a.traamt end as monto
from s_bana_productos.basig_dwba_ttran_hist_part a
join s_bana_productos.riesgodb_dwba_capcuentas b 
    on a.traoac = b.capnumcuenta
WHERE TRACDE = 'WD' --ORDER BY TRANEM;
and a.fch_proc >= '2019-01-01 00:00:00.0'
and b.capfchproceso = '2020-06-30 00:00:00.0'
union ALL
select  fch_proc fecha_transaccion, 
        'Salida' Tipo_trx,
       'SWIFT' nombre_canal, 
       'Transferencia Intl' accion_canal,
       'Salida Transferencia Intl' contexto,
        b.capcodcliente numero_unico,
        case when a.traamt < a.tranet then a.tranet else a.traamt end as monto
from s_bana_productos.basig_dwba_ttran_hist_part a
join s_bana_productos.riesgodb_dwba_capcuentas b 
    on a.traoac = b.capnumcuenta
WHERE TRACDE = 'WC' --ORDER BY TRANEM;
and fch_proc >= '2019-01-01 00:00:00.0'
and b.capfchproceso = '2020-06-30 00:00:00.0'

--LBTR
with out_lbtr as (
    select *, 'Nota de Cargo BCR' accion_canal--distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2019-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
        and tracde = 'DB'
        and tranem like 'ND%'
        and trabth = 2308
    union all 
    select *, 'Traslado Automático BCR' accion_canal--distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2019-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
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
    where a.fch_proc >= '2019-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
        and tracde = 'CR'
        and tranem like 'PPRV%'
        and trabth = 1416
        and traacc !=0
    union all
    select 'Nota de Abono LBTR' accion_canal, * --distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2019-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
        and tracde = 'CR'
        and tranem like 'LBTR%'
        and trabth = 1401
        and traacc != 0
    union all
    select 'Nota de Abono Manual LBTR' accion_canal, * --distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2019-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
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
