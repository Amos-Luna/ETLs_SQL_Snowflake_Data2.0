-- Do not change:
!set variable_substitution=true;
!set exit_on_error=true;

-- Destino: -
-- Fuentes: ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_DETAILEVENT
--          ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_DETAILOFFER
--          ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_HANDLEOFFERCONSULTANT
--          ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_HANDLEPARAMETERS
--          ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_PARAMETERSOFFERPRODUCTS
--       ***ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_CONSULTANTSEGMENT

--  Reprocesable : Si
--  Descripcion  : Valida si existe duplicados

-- Parameters:
SET V_CODPAIS = '&{P_CODPAIS}';
SET P_MODULO = '&{P_MODULO}';
SET P_ENVIRONMENT = '&{P_ENVIRONMENT}';
SET P_IDCARGA = '&{P_IDCARGA}';
SET P_UUIDFILE = '&{P_UUIDFILE}';
SET P_IDBITACORA = '&{P_IDBITACORA}';
SET P_FILE = '&{P_FILE}';
SET P_IDCABECERABITACORA = '&{P_IDCABECERABITACORA}';

-- Integrity validation variables:
SET Var_NroRegistrosRepetidos = 0;

/* TODO: La llave de las tablas se validan a traves de la PK */

---------------------------------------------------
------------------ DETAILEVENT --------------------
---------------------------------------------------
SET Var_NroRegistrosRepetidos = (SELECT COUNT(1)
FROM (
	SELECT 	CODPAIS, ANIOCAMPANA
	FROM 	ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_DETAILEVENT
	GROUP BY CODPAIS, ANIOCAMPANA
	HAVING COUNT(1) > 1 ));

SELECT ENT_&{P_ENVIRONMENT}_COMMON_DB.ETL_OBJECTS.UDF_VALIDA_INCONSISTENCIA($Var_NroRegistrosRepetidos, 'Existen duplicados en CODPAIS, ANIOCAMPANA dentro de Detailevent');

---------------------------------------------------
------------------ DETAILOFFER --------------------
---------------------------------------------------
SET Var_NroRegistrosRepetidos = (SELECT COUNT(1)
FROM (
	SELECT 	CODPAIS, ANIOCAMPANA
	FROM 	ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_DETAILOFFER
	GROUP BY CODPAIS, ANIOCAMPANA
	HAVING COUNT(1) > 1 ));

SELECT ENT_&{P_ENVIRONMENT}_COMMON_DB.ETL_OBJECTS.UDF_VALIDA_INCONSISTENCIA($Var_NroRegistrosRepetidos, 'Existen duplicados en CODPAIS, ANIOCAMPANA dentro de Detailoffer');

-------------------------------------------------------------
------------------ HANDLEOFFERCONSULTANT --------------------
-------------------------------------------------------------
SET Var_NroRegistrosRepetidos = (SELECT COUNT(1)
FROM (
	SELECT 	CODPAIS, ANIOCAMPANA, CODIGOPALANCA
	FROM 	ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_HANDLEOFFERCONSULTANT
	GROUP BY CODPAIS, ANIOCAMPANA, CODIGOPALANCA
	HAVING COUNT(1) > 1 ));

SELECT ENT_&{P_ENVIRONMENT}_COMMON_DB.ETL_OBJECTS.UDF_VALIDA_INCONSISTENCIA($Var_NroRegistrosRepetidos, 'Existen duplicados en CODPAIS, ANIOCAMPANA, CODIGOPALANCA dentro de Handleofferconsultant');

--------------------------------------------------------
------------------ HANDLEPARAMETERS --------------------
--------------------------------------------------------
SET Var_NroRegistrosRepetidos = (SELECT COUNT(1)
FROM (
	SELECT 	CODPAIS, ANIOCAMPANA, CODIGOPALANCA
	FROM 	ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_HANDLEPARAMETERS
	GROUP BY CODPAIS, ANIOCAMPANA, CODIGOPALANCA
	HAVING COUNT(1) > 1 ));

SELECT ENT_&{P_ENVIRONMENT}_COMMON_DB.ETL_OBJECTS.UDF_VALIDA_INCONSISTENCIA($Var_NroRegistrosRepetidos, 'Existen duplicados en CODPAIS, ANIOCAMPANA, CODIGOPALANCA dentro de Handleparameters');

---------------------------------------------------------------
------------------ PARAMETERSOFFERPRODUCTS --------------------
---------------------------------------------------------------
SET Var_NroRegistrosRepetidos = (SELECT COUNT(1)
FROM (
	SELECT 	CODPAIS, ANIOCAMPANA, CODIGOPALANCA, OFFERPARAMETERID
	FROM 	ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_PARAMETERSOFFERPRODUCTS
	GROUP BY CODPAIS, ANIOCAMPANA, CODIGOPALANCA, OFFERPARAMETERID
	HAVING COUNT(1) > 1 ));

SELECT ENT_&{P_ENVIRONMENT}_COMMON_DB.ETL_OBJECTS.UDF_VALIDA_INCONSISTENCIA($Var_NroRegistrosRepetidos, 'Existen duplicados en CODPAIS, ANIOCAMPANA, CODIGOPALANCA, OFFERPARAMETERID dentro de Parametersofferproducts');

---------------------------------------------------------------
------------------ CONSULTANTSEGMENT --------------------
---------------------------------------------------------------
/*
SET Var_NroRegistrosRepetidos = (SELECT COUNT(1)
FROM (
	SELECT 	CODPAIS, ANIOCAMPANA
	FROM 	ENT_&{P_ENVIRONMENT}_LANDING_DB.TRANSITORY.CM_&{P_CODPAIS}_CONSULTANTSEGMENT
	GROUP BY CODPAIS, ANIOCAMPANA
	HAVING COUNT(1) > 1 ));

SELECT ENT_&{P_ENVIRONMENT}_COMMON_DB.ETL_OBJECTS.UDF_VALIDA_INCONSISTENCIA($Var_NroRegistrosRepetidos, 'Existen duplicados en CODPAIS, ANIOCAMPANA dentro de Consultantsegment');
*/




