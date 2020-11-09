create table proceso_bana_vbeyg.huella_x_sector_origen stored as parquet as 
with transacciones_cliente as (
select substr(cast (a.sk_fecha_transaccion as string),1,6) sk_mes_trx, 
        a.sk_cliente_origen, 
        b.sectorba sector_origen,
        b.subsector subsector_origen,
        b.cluster cluster_origen,
        b.tipo_grupo,
        b.gerenciamiento,
        b.cde,
        b.flag_relacion,
        a.sk_cliente_destino, 
        case when a.sk_cliente_origen = a.sk_cliente_destino then 'MISMO ÚNICOD DESTINO'
        ELSE 'DISTINTO ÚNICO' END AS flag_comprobante, 
        a.nombre_canal, 
        a.accion_canal,
        a.contexto,
        sum(a.monto) monto,
        count(1) num_trx
from proceso_bana.veg_huella_trx a join 
proceso_bana_vbeyg.tabla_sectores b 
on a.sk_cliente_origen = b.no_unico
where a.sk_fecha_transaccion >= 20190101
group by substr(cast (a.sk_fecha_transaccion as string),1,6), 
        a.sk_cliente_origen, 
        b.sectorba,
        b.subsector,
        b.cluster,
        b.tipo_grupo,
        b.gerenciamiento,
        b.cde,
        b.flag_relacion,
        a.sk_cliente_destino, 
        case when a.sk_cliente_origen = a.sk_cliente_destino then 'MISMO ÚNICOD DESTINO'
        ELSE 'DISTINTO ÚNICO' END, 
        a.nombre_canal, 
        a.accion_canal,
        a.contexto
) 
select a.sk_mes_trx, a.sector_origen, a.subsector_origen, a.cluster_origen, a.gerenciamiento, a.cde, 
       a.flag_comprobante, a.nombre_canal, a.accion_canal, a.contexto, 
       sum(a.monto) monto_tranzado,
       sum(a.num_trx) numero_trxs,
       count(distinct a.sk_cliente_destino) clientes
from transacciones_cliente a
group by a.sk_mes_trx, a.sector_origen, a.subsector_origen, a.cluster_origen, a.gerenciamiento, a.cde, 
       a.flag_comprobante, a.nombre_canal, a.accion_canal, a.contexto
;