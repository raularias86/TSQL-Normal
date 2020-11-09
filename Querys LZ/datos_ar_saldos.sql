/***************QUERY FINAL***********/
CREATE TABLE PROCESO_BANA_vbeyg.datos_ar_saldos stored as parquet as 
WITH SALDOS_CLIENTES_X_AR AS (
    select a.agrcun NO_UNICO_AR, a.nombre_ar, a.COLAGENTE COD_AR, 
        CASE WHEN A.COLDIASMORA = 0 THEN "1. SIN MORA" 
        WHEN A.COLDIASMORA >0 AND A.COLDIASMORA <= 30 THEN "2. MORA 30" 
        WHEN A.COLDIASMORA >30 THEN "3. MORA +30" END RANGO_DIAS_MORA_EMPLEADOS, 
        count(distinct a.cifcodcliente) NO_EMPLEADOS, SUM(A.SALDO) SALDO_EMPLEADOS, SUM(A.MONTO) MONTO_EMPLEADOS
    from proceso_bana.cliente_ar a
    GROUP BY  a.agrcun, a.nombre_ar, a.COLAGENTE, 
    CASE WHEN A.COLDIASMORA = 0 THEN "1. SIN MORA" 
        WHEN A.COLDIASMORA >0 AND A.COLDIASMORA <= 30 THEN "2. MORA 30" 
        WHEN A.COLDIASMORA >30 THEN "3. MORA +30" END
) AR_UNICOS (
    SELECT DISTINCT NO_UNICO_AR
    FROM SALDOS_CLIENTES_X_AR 
    WHERE NO_UNICO_AR != 0 AND NO_UNICO_AR IS NOT NULL 
)
,SALDOS_AR_X_UNICO (
    SELECT A.NO_UNICO_AR, B.CLIENTE, B.cod_area_financiera, B.AREA_FINANCIERA, B.cod_asignado,
        COUNT(B.no_cuenta) NO_REFERENCIAS, SUM(B.SALDO) SALDO_AR, sum(B.MONTO) MONTO, MAX(B.dias_mora_cap) MAX_MORA_CAP, 
        MAX(B.dias_mora_interes) MAX_MORA_INT, 
        MAX(B.dias_mora_cap) DIAS_MORA_MAX
    FROM AR_UNICOS A LEFT OUTER JOIN 
        s_bana_productos.basig_dashba_portafolio_d B ON  A.NO_UNICO_AR = B.no_unico
    WHERE YEAR = 2020 AND YEAR(B.fch_proceso) = 2020 AND month(B.fch_proceso) = 3 AND DAY(B.fch_proceso) = 31 --= '2020-03-31'
        AND B.status != 'S'
        AND B.segment1 = '1. PORTAFOLIO PRESTAMOS'
    GROUP BY A.NO_UNICO_AR, A.NOMBRE_AR, A.COD_AR, A.RANGO_DIAS_MORA_EMPLEADOS, A.NO_EMPLEADOS, 
        A.SALDO_EMPLEADOS, A.MONTO_EMPLEADOS,
        B.no_unico, B.cod_area_financiera, B.cod_asignado
)

;

