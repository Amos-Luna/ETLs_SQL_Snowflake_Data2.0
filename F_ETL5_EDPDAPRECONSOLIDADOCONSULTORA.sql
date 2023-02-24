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
--                              INPUT 1
--                            Domain
--      df_edp_asignacion_id_consultora // edpda_asignacionidconsultora
-- #####################################################################
CREATE TEMPORARY TABLE df_edp_asignacion_id_consultora AS (

    SELECT *
    FROM (
        SELECT 
            codpais,
            aniocampana,
            codebelista,
            idoferta,
            orden,
            TRIM(tactica) as tactica,
            inputorigen,
            idofertaorigen,
            reactioncandidateid,
            createddatesimulationreaction,
            oddcandidateid,
            dia,
            procedencia,
            flgexposprior,
            ROW_NUMBER() OVER (PARTITION BY codpais, aniocampana, codebelista, idoferta, oddcandidateid, dia
                                ORDER BY inputorigen DESC NULLS LAST,
                                        orden ASC NULLS LAST,
                                        flgexposprior DESC NULLS LAST,
                                        idofertaorigen ASC NULLS LAST) AS fila
        FROM edpda_asignacionidconsultora
        WHERE codpais = $V_CODPAIS
            AND UUIDFILE = $P_UUIDFILE
        ) AS temp
    WHERE fila = 1

)

-- #####################################################################
--                              INPUT 2 
--                            TableFunctional
--              df_corp_det_planit_digital // corp_det_planit_digital
-- #####################################################################

CREATE TEMPORARY TABLE df_corp_det_planit_digital AS (

    SELECT 
        codpais,
        TRIM(aniocampana) AS aniocampana,
        CONCAT(fechaproceso, ' 16:00:00') AS fechaproceso,
        dispositivoajustado,
        codregion,
        codzona,
        TRIM(codebelista) AS codebelista,
        desunidadnegocio,
        segmentacioncaminobrillante,
        ipunicozona,
        realvtamnneto,
        realvtadolneto,
        realuuvendidas,
        realcostomnvendidas,
        realcostodolvendidas,
        desproductocuc,
        codcuc AS cuc,
        TRIM(codventa) AS codventa,
        TRIM(palancaajustado) AS palancaajustado,
        desmedioventaajustado,
        grupocompraganamas,
        NULL AS desclaseajustado,
        formatoplan AS formato,
        codestrategia,
        numoferta AS nrooferta,
        cantidadpedidospais,
        TRIM(aniocampanaref) AS aniocampanaref,
        flagventacomportamiento,
        desespacio,
        despopup,
        codseccion,
        constancia,
        segmentacionrolling AS descomportamiento,
        codsap
    FROM corp_det_planit_digital
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE
        AND flagventacomportamiento = 1 
        AND realuuvendidas > 0

)

-- #####################################################################
--                              INPUT 3 
--                            Curated
--              df_tmp_temporal_3 // edptmp_consolidadoconsultora
-- #####################################################################
CREATE TEMPORARY TABLE df_tmp_temporal_3 AS (

    SELECT *
    FROM edptmp_consolidadoconsultora
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE

)

------------------ INICIO df_plan_perfil_x_sinplaneacion_arp ------------------
CREATE TEMPORARY TABLE df_plan_perfil_x_sinplaneacion_arp AS (

    SELECT DISTINCT 
        codpais, 
        aniocampana, 
        codebelista 
    FROM df_edp_asignacion_id_consultora 
    WHERE inputorigen = 'ARP'

)

------------------ INICIO df_plan_perfil_x_sinplaneacion_cdp ------------------
CREATE TEMPORARY TABLE df_plan_perfil_x_sinplaneacion_cdp AS (

    SELECT DISTINCT 
        codpais, 
        aniocampana, 
        codebelista 
    FROM df_corp_det_planit_digital

)

------------------ INICIO df_plan_perfil_x_sinplaneacion ------------------
CREATE TEMPORARY TABLE df_plan_perfil_x_sinplaneacion AS (

    SELECT DISTINCT * 
    FROM (
        SELECT * FROM df_plan_perfil_x_sinplaneacion_arp
        UNION
        SELECT * FROM df_plan_perfil_x_sinplaneacion_cdp
    ) t

)

------------------ INICIO df_plan_perfil_x ------------------
CREATE TEMPORARY TABLE df_plan_perfil_x AS (

    SELECT 
        sp.codpais,
        sp.aniocampana, 
        sp.codebelista
    FROM df_plan_perfil_x_sinplaneacion sp
    WHERE sp.codebelista_cp IS NULL

)


------------------ INICIO df_tmp_temporal_4_sinperfilx ------------------
CREATE TEMPORARY TABLE df_tmp_temporal_4_sinperfilx AS (

    SELECT 
        tmp.codpais,
        tmp.aniocampana,
        tmp.codebelista,
        tmp.cuc,
        tmp.precionormalunitmn,
        tmp.precioofertaunitmn,
        tmp.unidades,
        tmp.codventa,
        tmp.codsap,
        tmp.desmarca,
        tmp.desclase AS desclase,
        tmp.desunidadnegocio,
        tmp.destipo,
        tmp.desproducto,
        tmp.descripcuc,
        tmp.segmentorollingplan,
        tmp.grupocompraplan,
        tmp.desregionplan,
        tmp.deszonaplan,
        tmp.fechaproceso,
        tmp.dispositivoajustado,
        tmp.desespacio,
        tmp.despopup,
        COALESCE(tmp.realvtamnneto, 0.00) AS realvtamnneto,
        COALESCE(tmp.realvtadolneto, 0.00) AS realvtadolneto,
        tmp.realuuvendidas,
        tmp.realcostomnvendidas,
        tmp.realcostodolvendidas,
        tmp.codestrategia,
        tmp.nrooferta,
        tmp.cantidadpedidospais,
        tmp.codtipooferta,
        tmp.realuufaltantes,
        tmp.realvtamnfaltneto,
        CASE
            WHEN tmp.flagmaterialganancia = TRUE THEN 'Replica Digital'
            WHEN CAST(tmp.codtipooferta AS INT) >= 200 AND CAST(tmp.codtipooferta AS INT) <= 228 THEN 'Exclusiva Digital'
            WHEN tmp.idoferta IS NULL THEN 'Impreso'
            ELSE 'Exclusiva Digital'
        END AS flagorigendigital,
        tmp.flagofertadigital,
        tmp.medioventa,
        tmp.flagresultados,
        tmp.flagreplicarevista,
        tmp.aniocampanaref,
        tmp.flagofertafinal,
        tmp.flagprodsugerido,
        tmp.codcatalogo,
        CASE
            WHEN tmp.codebelista = 'XXXXXXXXX' THEN TRUE
            WHEN dx.codebelista IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS flagperfilx,
        tmp.idoferta AS idoferta,
        tmp.inputorigen,
        CAST(NULL AS BIGINT) AS idofertaorigen,
        CAST(NULL AS BIGINT) AS reactioncandidateid,
        tmp.createddatesimulationreaction,
        tmp.oddcandidateid,
        COALESCE(tmp.cucpadre, tmp.cuc) AS cucpadre,
        tmp.costounitmn,
        CASE
            WHEN tmp.aniocampana >= 202017 THEN tmp.formatooferta
            ELSE tmp.formatoplan
        END AS formatoplan,
        tmp.precionormalsetmn,
        tmp.precioofertasetmn,
        tmp.porcdescuentoset,
        tmp.costosetmn,
        tmp.flagmaterialganancia,
        COALESCE(tmp.flagcompuestavariable, 0) AS flagcompuestavariable,
        tmp.factorcuadre,
        tmp.codventapadre AS codventapadre,
        tmp.fmulticlase,
        COALESCE(tmp.claseoferta, 'N/A') AS claseoferta,
        tmp.fmultimarca,
        COALESCE(tmp.marcaoferta, 'N/A') AS marcaoferta,
        tmp.formatooferta,
        CASE 
            WHEN COALESCE(TRIM(tmp.descripcionoferta), 'N/A') = '' THEN 'N/A'
            ELSE COALESCE(TRIM(tmp.descripcionoferta), 'N/A') 
        END AS descripcionoferta,
        tmp.unidadesset,
        tmp.desmarcaoferta,
        tmp.descategoriaoferta,
        tmp.comentario,
        tmp.orden AS orden,
        tmp.tactica AS tactica,
        tmp.mindiaodd AS mindiaodd,
        tmp.maxdiaodd AS maxdiaodd,
        tmp.diasodd AS diasodd,
        tmp.flgexposprior AS flgexposprior,
        CASE WHEN tmp.inputorigen = 'ARP' THEN 'RESULTADOS' ELSE tmp.procedencia END AS procedencia,
        (COALESCE(tmp.precionormalunitmn, 0) * COALESCE(tmp.unidades, 0)) AS prioridad
    FROM df_tmp_temporal_3 tmp
    LEFT JOIN df_plan_perfil_x dx
    ON tmp.codpais = dx.codpais
        AND tmp.aniocampana = dx.aniocampana
        AND tmp.codebelista = dx.codebelista

)

------------------ INICIO df_tmp_temporal_4_conperfilx ------------------
CREATE TEMPORARY TABLE df_tmp_temporal_4_conperfilx AS (

    SELECT DISTINCT
        tmp.codpais,
        tmp.aniocampana,
        dx.codebelista,
        tmp.cuc,
        tmp.precionormalunitmn,
        tmp.precioofertaunitmn,
        tmp.unidades,
        tmp.codventa,
        tmp.codsap,
        tmp.desmarca,
        tmp.desclase AS desclase,
        tmp.desunidadnegocio,
        tmp.destipo,
        tmp.desproducto,
        tmp.descripcuc,
        tmp.segmentorollingplan,
        tmp.grupocompraplan,
        tmp.desregionplan,
        tmp.deszonaplan,
        tmp.fechaproceso,
        tmp.dispositivoajustado,
        tmp.desespacio,
        tmp.despopup,
        tmp.realvtamnneto,
        tmp.realvtadolneto,
        tmp.realuuvendidas,
        tmp.realcostomnvendidas,
        tmp.realcostodolvendidas,
        tmp.codestrategia,
        tmp.nrooferta,
        tmp.cantidadpedidospais,
        tmp.codtipooferta,
        tmp.realuufaltantes,
        tmp.realvtamnfaltneto,
        tmp.flagorigendigital,
        tmp.flagofertadigital,
        tmp.medioventa,
        tmp.flagresultados,
        tmp.flagreplicarevista,
        tmp.aniocampanaref,
        tmp.flagofertafinal,
        tmp.flagprodsugerido,
        tmp.codcatalogo,
        tmp.flagperfilx,
        tmp.idoferta,
        tmp.inputorigen,
        tmp.idofertaorigen,
        tmp.reactioncandidateid,
        tmp.createddatesimulationreaction,
        tmp.oddcandidateid,
        tmp.cucpadre,
        tmp.costounitmn,
        tmp.formatoplan,
        tmp.precionormalsetmn,
        tmp.precioofertasetmn,
        tmp.porcdescuentoset,
        tmp.costosetmn,
        tmp.flagmaterialganancia,
        tmp.flagcompuestavariable,
        tmp.factorcuadre,
        tmp.codventapadre,
        tmp.fmulticlase,
        tmp.claseoferta,
        tmp.fmultimarca,
        tmp.marcaoferta,
        tmp.formatooferta,
        tmp.descripcionoferta,
        tmp.unidadesset,
        tmp.desmarcaoferta,
        tmp.descategoriaoferta,
        tmp.comentario,
        tmp.orden,
        tmp.tactica,
        tmp.mindiaodd,
        tmp.maxdiaodd,
        tmp.diasodd,
        tmp.flgexposprior,
        tmp.procedencia,
        tmp.prioridad
    FROM df_tmp_temporal_4_sinperfilx tmp
    JOIN df_plan_perfil_x dx
        ON tmp.codpais = dx.codpais
        AND tmp.aniocampana = dx.aniocampana
    WHERE tmp.codebelista = 'XXXXXXXXX'

)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL_5 ----------------------------
----------------------- edppre_consolidadoconsultora ----------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================

CREATE TEMPORARY TABLE edppre_consolidadoconsultora AS (

    SELECT * 
    FROM df_tmp_temporal_4_sinperfilx 
    WHERE codebelista != 'XXXXXXXXX'
    UNION
    SELECT * 
    FROM df_tmp_temporal_4_conperfilx 
    WHERE codebelista != 'XXXXXXXXX'

)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL_5 ----------------------------
----------------------- edppre_consolidadoconsultora ----------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================