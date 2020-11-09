
--1. PRIMERO HACER UN DELETE DE LOS DATOS DE LA TABLA DE IVC

TRUNCATE TABLE BASIGBA08.DATA_EMPRESAS.DBO.BASE_IVC_VEG 

--2. HACER UN INSERT DE LOS DATOS

INSERT BASIGBA08.DATA_EMPRESAS.DBO.BASE_IVC_VEG 
(FCH_PROCESO, COD_AREA_FINANCIERA, COD_ASIGNADO, COD_MASTER, NO_UNICO, AGRUPACION_PRODUCTO,
COD_PRODUCTO, NOMBRE_PRODUCTO, NUMERO_REFERENCIAS)	
--TABLA DE SALDOS 
SELECT A.FCH_PROCESO, A.COD_AREA_FINANCIERA, A.COD_ASIGNADO, 
	   C.MASTER,
	   A.NO_UNICO,
	   --NULL TIPO_PRODUCTO,
	   CASE WHEN A.COD_PRODUCTO IN ('20106','20132','20107','20108','20117','20167','20168','20134') 
			THEN 'CASH MANAGER' 			
			ELSE B.AGRUPACION_PRODUCTO END AS AGRUPACION_PRODUCTO, 
	   A.COD_PRODUCTO,
	   B.NOMBRE_PRODUCTO,
	   COUNT(DISTINCT NO_CUENTA) NUMERO_REFERENCIAS
FROM SALDOS.DBO.SALDOS_2017 A JOIN DATA_EMPRESAS.DBO.CATALOGO_PRODUCTOS B 
ON A.COD_PRODUCTO = B.COD_PRODUCTO JOIN DATA_EMPRESAS.DBO.VW_CLIENTES_ULT_MES C ON 
A.NO_UNICO = C.NO_UNICO
WHERE A.FCH_PROCESO IN (SELECT MAX(FCH_PROCESO) FROM SALDOS.DBO.SALDOS_2017 GROUP BY MONTH(FCH_PROCESO)) 
AND FCH_PROCESO = '2019-12-31'
GROUP BY A.FCH_PROCESO,  A.COD_AREA_FINANCIERA, A.COD_ASIGNADO, 
	   C.MASTER, A.NO_UNICO,
	   CASE WHEN A.COD_PRODUCTO IN ('20106','20132','20107','20108','20117','20167','20168','20134') 
			THEN 'CASH MANAGER' ELSE B.AGRUPACION_PRODUCTO END, A.COD_PRODUCTO,
	   B.NOMBRE_PRODUCTO
UNION ALL
--TABLA DE "OTROS PRODUCTOS"
SELECT A.FCH_PROCESO, A.COD_AREA_FINANCIERA, A.COD_ASIGNADO, 
	   C.MASTER,
	   A.NO_UNICO,
	   --NULL TIPO_PRODUCTO,
	   CASE WHEN A.PRODUCT_ID IN ('20106','20132','20107','20108','20117','20167','20168','20134') 
			THEN 'CASH MANAGER' 			
			ELSE A.AGRUPADOR END AS AGRUPACION_PRODUCTO, 
	   A.PRODUCT_ID,
	   B.NOMBRE_PRODUCTO,
	   COUNT(DISTINCT NO_CUENTA) NUMERO_REFERENCIAS
FROM DATA_EMPRESAS.dbo.BASE_OTROS_PRODUCTOS A JOIN DATA_EMPRESAS.DBO.CATALOGO_PRODUCTOS B 
ON A.PRODUCT_ID = B.COD_PRODUCTO JOIN DATA_EMPRESAS.DBO.VW_CLIENTES_ULT_MES C ON 
A.NO_UNICO = C.NO_UNICO
WHERE A.FCH_PROCESO IN (SELECT MAX(FCH_PROCESO) FROM SALDOS.DBO.SALDOS_2017 GROUP BY MONTH(FCH_PROCESO)) 
AND FCH_PROCESO = '2019-12-31'
GROUP BY A.FCH_PROCESO,  A.COD_AREA_FINANCIERA, A.COD_ASIGNADO, 
	   C.MASTER, A.NO_UNICO,
	   CASE WHEN A.PRODUCT_ID IN ('20106','20132','20107','20108','20117','20167','20168','20134') 
			THEN 'CASH MANAGER' ELSE A.AGRUPADOR END, A.PRODUCT_ID,
	   B.NOMBRE_PRODUCTO