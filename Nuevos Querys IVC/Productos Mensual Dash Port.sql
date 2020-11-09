----QUERY MENSUAL PARA OTROS PRODUCTOS DESDE EL DASHPORTAFOLIO---------

SELECT A.FCH_PROCESO, B.BD_VICEPRECIDENTE, 
B.BK_AREA_FINANCIERA, B.BD_AREA_FINANCIERA, B.BK_EJECUTIVO, B.BD_EJECUTIVO,
CASE WHEN B.BK_EJECUTIVO IN ('24','19','10','698','679','619','199','116','111','508','507',
                             '506','505','502','913','95','94','92') THEN 'NO GERENCIADO'
                             ELSE 'GERENCIADO' END GERENCIAMIENTO,
B.BK_MASTER, B.BD_MASTER, A.NO_UNICO,B.BD_CLIENTE, A.SEGMENT2, C.COLAGRUPAPROD, 
CASE WHEN A.COD_PRODUCTO IN ('20106','20132','20107','20108','20117','20167','20168','20134') THEN 'PLANILLA'
		      ELSE A.SEGMENT2 END AGUPADOR_VAL,
A.COD_PRODUCTO, A.PRODUCTO, A.NO_CUENTA
FROM DASHBA.DASH_PORTAFOLIO_HIST@BASIG A JOIN DMDIMENSIONES.D_CLIENTE@BASIG B 
ON A.NO_UNICO = B.BK_CLIENTE LEFT OUTER JOIN BACATALOG.CATPRODUCTOS C
ON A.PRODUCTO = C.COLCODPRODUCTO
WHERE A.FCH_PROCESO >= '01/01/19'
AND BK_VICEPRECIDENTE = '1' 
;

SELECT  O268691.FCH_PROCESO, O268691.NO_UNICO, CLIENTE, COD_ASIGNADO, COD_AREA_FINANCIERA,
        O268691.SEGMENT1, SEGMENT2 ,NO_CUENTA, PRODUCTO, COD_PRODUCTO, MONTO, PLAZO_MESES, TASA_INTERES,
        FCH_APERTURA, FCH_VENCIMIENTO, SECTOR_ECONOMICO,DESTINO_BCR, SALDO SALDO_CONTABLE
       
FROM DASHBA.DASH_PORTAFOLIO_HIST@BASIG O268691
WHERE ( O268691.SEGMENT1 IN ('1. PORTAFOLIO PRESTAMOS','PORTAFOLIO DEPOSITOS') ) 
      AND ( O268691.FCH_PROCESO >= TO_DATE('20190601000000','YYYYMMDDHH24MISS') )
      AND ( O268691.STATUS != 'S')
	  AND ( O268691.COD_AREA_FINANCIERA IN (1103,1106,1113,1112,1111,1120,1123,1101))
    ;