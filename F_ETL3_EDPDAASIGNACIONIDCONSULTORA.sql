!set variable_substitution=true;
!set exit_on_error=true;

--  Destino: MODEL.CM - "Dominio de negocio"
--  Fuentes: ENT_&{P_ENVIRONMENT}_CURATED_DB.CM.CM_DETAILEVENT
--  Reprocesable : Si
--  Descripcion  : Agrega regsitros unicos

-- Parameters:

SET V_CODPAIS = '&{P_CODPAIS}';
SET P_MODULO = '&{P_MODULO}';
SET P_ENVIRONMENT = '&{P_ENVIRONMENT}';
SET P_IDCARGA = '&{P_IDCARGA}';
SET P_IDCABECERABITACORA = '&{P_IDCABECERABITACORA}';
SET P_INICIO_MODULO = '&{P_INICIO_MODULO}'; 
SET P_UUID_PROCESS = '&{P_UUID_PROCESS}';
SET P_UUIDFILE = '&{P_UUIDFILE}';

-- Procedure:
/* TO DO */

-- =========================================
-- -----------------------------------------
--      INICIO DE LÓGICA DE NEGOCIO
--  ----------------------------------------
-- =========================================

-- #####################################################################
--                              INICIO 
--      df_cm_handleofferconsultant // edpcm_handleofferconsultant
-- #####################################################################
CREATE TEMPORARY TABLE df_cm_handleofferconsultant AS (

    SELECT DISTINCT 
        codpais, 
        aniocampana, 
        codebelista, 
        CAST(offerid AS BIGINT) AS idoferta, 
        CAST(order AS BIGINT) AS orden, 
        CAST(reactioncandidateid AS BIGINT) AS reactioncandidateid, 
        createddatesimulationreaction, 
        CAST(oddcandidateid AS BIGINT) AS oddcandidateid, 
        CAST(day AS INT) AS dia, 
        CAST(reactionflag AS INT) AS reactionflag 
    FROM ENT_&{P_ENVIRONMENT}_CURATED_DB.CM.CM_HANDLEOFFERCONSULTANT 
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE
)

-- #####################################################################
--                              FIN 
--      df_cm_handleofferconsultant // edpcm_handleofferconsultant
-- #####################################################################

-- #####################################################################
--                              INICIO 
--              df_cm_detailoffer // edpcm_detailoffer
-- #####################################################################
CREATE TEMPORARY TABLE df_cm_detailoffer AS (

    SELECT DISTINCT 
        inputorigincode AS inputorigen, 
        TRIM(flagtactic) AS tactica, 
        CAST(offerid AS BIGINT) AS idoferta, 
        CAST(offeroriginid AS BIGINT) AS idofertaorigen, 
        aniocampana, 
        codpais 
    FROM ENT_&{P_ENVIRONMENT}_CURATED_DB.CM.CM_DETAILOFFER 
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE
)

-- #####################################################################
--                              FIN 
--              df_cm_detailoffer // edpcm_detailoffer
-- #####################################################################

------------------ INICIO df_pre_cons_id ------------------
CREATE TEMPORARY TABLE df_pre_cons_id AS (

    SELECT DISTINCT 
        h.codpais AS codpais, 
        h.aniocampana AS aniocampana, 
        h.idoferta AS idoferta,
        ------------------------------
        d.inputorigen AS inputorigen, 
        d.tactica AS tactica, 
        d.idofertaorigen AS idofertaorigen,
        h.codebelista AS codebelista, 
        h.orden AS orden, 
        h.createddatesimulationreaction AS createddatesimulationreaction, 
        h.reactioncandidateid AS reactioncandidateid, 
        h.oddcandidateid AS oddcandidateid, 
        h.dia AS dia, 
        h.reactionflag AS reactionflag 
    FROM df_cm_handleofferconsultant h 
    JOIN df_cm_detailoffer d 
    ON h.codpais = d.codpais 
        AND h.aniocampana = d.aniocampana 
        AND h.idoferta = d.offerid 
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE
)
------------------ FIN df_pre_cons_id ------------------

------------------ INICIO df_cons_id_0 ------------------
CREATE TEMPORARY TABLE df_cons_id_0 AS (

    SELECT 
        codpais, 
        aniocampana, 
        codebelista, 
        idoferta, 
        orden, 
        tactica, 
        inputorigen, 
        idofertaorigen, 
        reactioncandidateid, 
        createddatesimulationreaction, 
        oddcandidateid, 
        dia, 
        reactionflag, 
        ROW_NUMBER() OVER(  PARTITION BY codebelista, 
                                         orden 
                            ORDER BY codebelista ASC, 
                                        orden ASC, 
                                        inputorigen ASC, 
                                        tactica DESC NULLS LAST ) AS prior
    FROM df_pre_cons_id
)

------------------ FIN df_cons_id_0 ------------------

------------------ INICIO df_asignacionidconsultoracm ------------------
CREATE TEMPORARY TABLE df_asignacionidconsultoracm AS (

    SELECT 
        codpais, 
        aniocampana, 
        codebelista, 
        idoferta, 
        orden, 
        tactica, 
        inputorigen, 
        idofertaorigen, 
        reactioncandidateid, 
        createddatesimulationreaction, 
        oddcandidateid AS oddid, 
        dia, 
        CASE
            WHEN reactionflag = 1 THEN 'REACCION'
            WHEN reactionflag = 0 AND oddcandidateid IS NOT NULL THEN 'ODD'
            ELSE 'PLANEACION'
        END AS procedencia,
        CASE 
            WHEN prior = 1 THEN 1
            ELSE 0
        END AS flgexposprior
    FROM df_cons_id_0
)

------------------ FIN df_asignacionidconsultoracm ------------------


-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL3 ----------------------------
------------------- edpda_asignacionidconsultora -------------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================

CREATE TEMPORARY TABLE edpda_asignacionidconsultora AS (

    SELECT DISTINCT 
        *,
        ROW_NUMBER() OVER ( PARTITION BY codpais, 
                                        aniocampana, 
                                        codebelista, 
                                        idoferta, 
                                        oddcandidateid, 
                                        dia
                                ORDER BY inputorigen DESC NULLS LAST, 
                                        orden ASC NULLS LAST, 
                                        flgexposprior DESC NULLS LAST, 
                                        idofertaorigen ASC NULLS LAST ) AS fila
        '&{P_IDCARGA}'  AS IDCARGA, ---- ??????
        '&{P_UUID_PROCESS}' AS UUIDFILE -----????? FALTA FECPROCESO ???

    FROM df_asignacionidconsultoracm
    WHERE fila = 1
)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL3 -----------------------------
------------------- edpda_asignacionidconsultora -------------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================