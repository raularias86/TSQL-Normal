SELECT FCH_PROCESO, COD_AREA_FINANCIERA, SECTOR_ECONOMICO, DESTINO_BCR, 
       CIFTIPOCLIENT, CIFCODACTIVID,
       SUM(SALDO) SALDO_CONT, SUM(SALDO_REFERENCIA) SALDO_REF
FROM DASHBA.DASH_PORTAFOLIO_HIST@BASIG O268691 LEFT JOIN 
     DWBA.CIFGENERALES@BARIESGOS B ON O268691.NO_UNICO = B.CIFCODCLIENTE   
WHERE B.CIFFECHAPROCESO  = (SELECT MAX(CIFFECHAPROCESO) FROM DWBA.CIFGENERALES@BARIESGOS)
    and ( O268691.SEGMENT1 IN ('1. PORTAFOLIO PRESTAMOS')) --,'PORTAFOLIO DEPOSITOS') ) 

      AND ( O268691.FCH_PROCESO IN (TO_DATE('2018013100000','YYYYMMDDHH24MISS'),TO_DATE('2018022800000','YYYYMMDDHH24MISS'),
                    TO_DATE('2018033100000','YYYYMMDDHH24MISS'),TO_DATE('2018043000000','YYYYMMDDHH24MISS'),TO_DATE('2018053100000','YYYYMMDDHH24MISS'),
                    TO_DATE('2018063000000','YYYYMMDDHH24MISS'),TO_DATE('2018073100000','YYYYMMDDHH24MISS'),TO_DATE('2018083100000','YYYYMMDDHH24MISS')))-->
      AND ( O268691.STATUS != 'S')
GROUP BY FCH_PROCESO, COD_AREA_FINANCIERA, SECTOR_ECONOMICO, DESTINO_BCR, 
       CIFTIPOCLIENT, CIFCODACTIVID
;


SELECT DISTINCT FCH_PROC, TRABDY A�O, TRABDM MES, TRABDD DIA,TRAACC NO_CUENTA, TRAAMT MONTO_TRX, 
       TRACDE TRANSACCION_PORTAL, TRANEM APROBADO_PORTAL, TRACUN NO_UNICO, TRAOAC NO_CUENTA_DESTINO
--SELECT * --COUNT(1)
FROM DWBA.TTRAN_HIST@BASIG WHERE TRAACC = 5008607249
AND TRAVDY = 18
AND TRAVDM = 9
AND TRABDD = 14
--AND TRAAMT = 7380000
--GROUP BY FCH_PROC
ORDER BY FCH_PROC

