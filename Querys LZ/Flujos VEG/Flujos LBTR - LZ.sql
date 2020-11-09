create table flujo_lbtr stored as parquet as 
with lbtr_salida as (
    select *, 'Nota de Cargo BCR' accion_canal--distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2020-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
        and tracde = 'DB'
        and tranem like 'ND%'
        and trabth = 2308
    union all 
    select *, 'Traslado Automático BCR' accion_canal--distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2020-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
        and tracde = 'DB'
        and tranem like 'TBCR%'
        and trabth = 1402
) 
select  a.fch_proc fecha_transaccion, 
        'Salida' tipo_trx,
        'LBTR' canal, 
        a.accion_canal, 
        'Salida vía LBTR' contexto,
        b.capcodcliente, 
        a.traamt monto
        --travdm, travdy, tracde, tranem, sum(traamt) monto, count(1) trxs
from lbtr_salida a 
join s_bana_productos.riesgodb_dwba_capcuentas b 
    on a.traoac = b.capnumcuenta
where b.capfchproceso = '2020-06-30 00:00:00.0'    
--group by travdm, travdy, tracde, tranem
order by 1 desc, 6 -- travdm, travdy, tracde, tranem
;
---------------------
with lbtr_entrada as (
    select 'Pago de Ministerio' accion_canal, *--distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2020-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
        and tracde = 'CR'
        and tranem like 'PPRV%'
        and trabth = 1416
    union all
    select 'Nota de Abono LBTR' accion_canal, * --distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2020-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
        and tracde = 'CR'
        and tranem like 'LBTR%'
        and trabth = 1401
    union all
    select 'Nota de Abono Manual LBTR' accion_canal, * --distinct trabth, tracde, tranem
    from s_bana_productos.basig_dwba_ttran_hist_part a
    where a.fch_proc >= '2020-01-01 00:00:00.0' --and a.fch_proc <= '2020-01-31 00:00:00.0'
        and tracde = 'CR'
        and tranem like 'NA%'
        and trabth in (2311, 2309)
)
select  a.fch_proc fecha_transaccion, 
        'Entrada' tipo_trx,
        'LBTR' canal, 
        a.accion_canal, 
        'Entrada vía LBTR' contexto,
        b.capcodcliente, 
        a.traamt monto
from lbtr_entrada a 
join s_bana_productos.riesgodb_dwba_capcuentas b 
    on a.traoac = b.capnumcuenta
where b.capfchproceso = '2020-06-30 00:00:00.0'    
--group by travdm, travdy, tracde, tranem
order by 1 desc, 6 -- travdm, travdy, tracde, tranem

;