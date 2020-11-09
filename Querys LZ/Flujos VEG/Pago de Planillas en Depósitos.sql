---PAGO EN DEPÓSITOS 

select 'Pago Planilla en Depósitos' accion_canal, A.* 
from DWBA.TTRAN_HIST@BASIG A
where a.fch_proc >= '01/01/20' --and a.fch_proc <= '30/06/20'
and tracde = 'DB'
and tranem like 'PLA%'
and trabth = 3700
order by fch_proc desc, tracun