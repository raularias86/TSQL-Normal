---QUERY ACH ENTRADA CON NÚMERO ÚNICO

select a.tljvdy, a.tljrdm, b.numunico, 'ACH - ENTRADA' CANAL,sum(a.tljbca) MONTO
from DWBA.TLJRN_HIST@BASIG A LEFT JOIN STAGEBA.REFERENCIAS@BASIG B ON A.TLJOAC = B.REFERENCIA
--HABRÍA QUE HACER UN JOIN CON LA TABLA DE CLIENTES PARA TENER LA INFORMACIÓN DE LOS CLIENTES (ÁREA FINANCIERA Y DEMÁS)
where fch_proc  >= (select max(fch_proc) from DWBA.TLJRN_HIST@BASIG) --Para traer las ACH del día de ayer
and tljnem like 'ACH '  --Nemónico de las entradas vía ACH
AND TLJTTP = 'DP'
group by a.tljvdy, a.tljrdm, b.numunico
;