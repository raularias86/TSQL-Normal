--QUERY PARA VER LAS TRX INTERNACIONALES 
select anio_mes, sk_cliente,
       case when sk_tipo_transferencia= 1 then 'RECIBIDA' ELSE 'ENVIADA' END AS tipo_transferencia, 
       sum(monto_transaccion) monto_trx
from proceso_bana.f_transacciones_cump_swift
--SOLAMENTE FALTARÍA HACER UN JOIN EN LA DM CLIENTES Y FECHAS PARA PODER DETALLAR EL ORIGEN Y DESTINO DE LAS TRX
where sk_fecha >= 20190101
group by anio_mes, sk_cliente, case when sk_tipo_transferencia= 1 then 'RECIBIDA' ELSE 'ENVIADA' END
;