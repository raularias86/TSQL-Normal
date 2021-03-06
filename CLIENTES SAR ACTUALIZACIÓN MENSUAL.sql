
--- 1. INSERTANDO EN TABLA DE CLIENTES_VBEG LOS NUEVOS CLIENTES
insert into BASIGBA08.DATA_EMPRESAS.dbo.CLIENTES_VBEG 
			(NO_UNICO, NOMBRE_CLIENTE, COD_ASIGNADO,NOMBRE_ASIGNADO, AREA_FIN,COD_AREA_FINANCIERA)
select NO_UNICO, NOMBRE_CLIENTE, COD_ASIGNADO, '' NOMBRE_ASIGNADO, AREA_FIN, COD_AREA_FIN COD_AREA_FINANCIERA
from VW_CLIENTES_EMPRESAS
where FECHA_PROCESO = (select max(fecha_proceso) from VW_CLIENTES_EMPRESAS) 
and NO_UNICO not in (select distinct NO_UNICO 
					from  BASIGBA08.DATA_EMPRESAS.dbo.CLIENTES_VBEG) 
ORDER BY COD_AREA_FIN, COD_ASIGNADO, NO_UNICO 


-- 2. ACTUALIZANDO LA TABLA DE CLIENTES_VBEG CON LAS �REAS FINANCIERAS Y C�DIGOS DE ASIGNADO CORRESPONDIENTES
UPDATE A
SET A.AREA_FIN = B.AREA_FIN, 
	A.COD_AREA_FINANCIERA = B.COD_AREA_FIN, 
	A.COD_ASIGNADO = B.COD_ASIGNADO
from  BASIGBA08.DATA_EMPRESAS.dbo.CLIENTES_VBEG A JOIN CLIENTES B
ON A.NO_UNICO = B.NO_UNICO
WHERE B.FECHA_PROCESO = '2017-10-31'


--- 3. BORRAR CLIENTES QUE HAYAN QUEDADO EN �REAS FINANCIERAS QUE YA  NO CORRESPONDEN A EMPRESAS
DELETE FROM BASIGBA08.DATA_EMPRESAS.dbo.CLIENTES_VBEG 
WHERE COD_AREA_FINANCIERA IN (1519,1121,1214,1520,1199,1521,1524,1200)

--- 4. ACTUALIZAR NOMBRES DE GERENTES DE CUENTA, USANDO EL LISTADO DE CLIENTES DE GESTI�N EXTRAER LOS NOMBRES Y HACER UN UPDATE:
--UPDATE BASIGBA08.DATA_EMPRESAS.dbo.CLIENTES_VBEG SET NOMBRE_ASIGNADO = 'TERESA MERCEDES BOLA#OS       ' WHERE COD_ASIGNADO = '1'
--CAMBIARLO CON LOS NUEVOS, ESTE ES SOLO UN EJEMPLO

--- 5. ACTUALIZAR NUEVAMENTE LOS NOMBRES DE LOS GTES DE CUENTA, QUITANDO LOS ESPACIOS EN BLANCO A LA DERECHA DE ESOS NOMBRES. 

UPDATE BASIGBA08.DATA_EMPRESAS.dbo.CLIENTES_VBEG 
SET NOMBRE_ASIGNADO = RTRIM(NOMBRE_ASIGNADO), 
	NOMBRE_CLIENTE  = RTRIM(NOMBRE_CLIENTE)



UPDATE BASIGBA08.DATA_EMPRESAS.dbo.SARC_HIST
SET S1_Asignado = RTRIM(S1_ASIGNADO),
	S1_Cliente = RTRIM(S1_CLIENTE) 
