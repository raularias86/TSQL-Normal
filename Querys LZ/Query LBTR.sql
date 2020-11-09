-----SALIDA DE FONDOS VÍA LBTR
select *
from DWBA.TTRAN_HIST@BASIG A
where a.fch_proc >= '01/01/20' --and a.fch_proc <= '30/06/20'
and tracde = 'DB'
and tranem like 'TBCR'
order by fch_proc desc, tracde, tranem;

-----ENTRADA DE FONDOS VÍA CUT
--tipología 1 Lote 1416 es el de entrada por CUT CR-PPRV (pago de proveedores), PLA(PLANILLA), MH (PAGO DE RENTA)

select * ---DISTINCT TRABTH, TRACDE, TRANEM
from DWBA.TTRAN_HIST@BASIG A
where a.fch_proc >= '01/07/20' --and a.fch_proc <= '30/06/20'
and tracde = 'CR'
and tranem like 'PPRV%'
and trabth = 1416
order by fch_proc desc, tracde, tranem; --TRABTH, TRACDE, TRANEM; --

--tipología 2 Lote 1401, la oficina es 999, porque es algo automático: CR-LBTR
select * --DISTINCT TRABTH, TRACDE, TRANEM
from DWBA.TTRAN_HIST@BASIG A
where a.fch_proc >= '01/07/20' --and a.fch_proc <= '30/06/20'
and tracde = 'CR'
and tranem like 'LBTR%'
and trabth = 1401
order by fch_proc desc, tracde, tranem;  -- TRABTH, TRACDE, TRANEM; --

--tipología 3 3 (es el de la tipología 2 pero de forma manual)CR-NA y con lote 2309 y oficina 016
select *-- DISTINCT TRABTH, TRACDE, TRANEM
from DWBA.TTRAN_HIST@BASIG A
where a.fch_proc >= '01/07/20' --and a.fch_proc <= '30/06/20'
and tracde = 'CR'
and tranem like 'NA%'
and trabth in (2311, 2309)
--traobr = 16
order by  fch_proc desc, traamt;  --TRABTH, TRACDE, TRANEM; -- , tracde, tranem