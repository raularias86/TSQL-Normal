select fch_proc, sum(traamt) montos, count(distinct trackn) referencias
    FROM DWBA.TTRAN_HIST@BASIG A 
    where fch_proc >= '09/04/20'
    --and A.TRAACC = '3080775040'
    and tratlr = 0001 ---lote 0001 según portal para fsv y para aduana
    and TRACDE= 'DB' --es un débito a la cuenta
    --TRANEM= IMPT nmónico para cuenta, en tljr es igual
    --and fch_proc <= '31/12/19'
    and tranem = 'IMPT'
    group by fch_proc
    ;

    select   A.* --SUM(TLJNCH) SALDO -- count(1)
from DWBA.TLJRN_HIST@BASIG A
where tljrdy = 2020
and tljrdm = 4
--and tljvdd = 6
and tljoac = '5030477477'
--and tljoac = '3080775040'
--and tljtlr = 0001
and tljnem = 'IMPT'
and tljttp = 'RD'
--and tljbca = 1290.51
;