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
--                              INPUT
--                            Domain
--      df_edp_asignacion_id_consultora // edpda_asignacionidconsultora
-- #####################################################################
CREATE TEMPORARY TABLE df_edp_asignacion_id_consultora AS (

    CREATE TEMPORARY TABLE edpda_asignacionidconsultora_aux_tab AS (
        SELECT DISTINCT
            codpais,
            aniocampana,
            codebelista,
            idoferta,
            orden,
            trim(tactica) as tactica,
            inputorigen,
            idofertaorigen,
            reactioncandidateid,
            createddatesimulationreaction,
            oddcandidateid,
            dia,
            procedencia,
            flgexposprior,
            ROW_NUMBER() OVER ( PARTITION BY 
                                    codpais, 
                                    aniocampana, 
                                    codebelista, 
                                    idoferta, 
                                    oddcandidateid, 
                                    dia
                                ORDER BY 
                                    inputorigen DESC NULLS LAST, 
                                    orden ASC NULLS LAST, 
                                    flgexposprior DESC NULLS LAST, 
                                    idofertaorigen ASC NULLS LAST
            ) AS fila
        FROM edpda_asignacionidconsultora
        WHERE codpais = $V_CODPAIS
            AND UUIDFILE = $P_UUIDFILE
    )
    SELECT *
    FROM edpda_asignacionidconsultora_aux_tab
    WHERE fila = 1;
)

-- #####################################################################
--                              INPUT 
--                            DomReportes
--              df_tmp_temporal_5 // edppre_consolidadoconsultora !!!(tabla generada en ETL_5)!!!
-- #####################################################################

CREATE TEMPORARY TABLE df_tmp_temporal_5 AS (

    SELECT *
    FROM edppre_consolidadoconsultora
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE
)

-- #####################################################################
--                              INPUT 
--                            DomReportes
--              df_det_consultora360 // det_consultora_360
-- #####################################################################
CREATE TEMPORARY TABLE df_det_consultora360 AS (

    SELECT 
        cod_pais,
        cod_aniocampana,
        cod_consultora,
        bnd_ip_unicozona,
        des_status_comercial,
        des_comportamiento,
        bnd_paso_pedido,
        bnd_pasopedido_web,
        cod_seccion,
        bnd_activa
    FROM det_consultora_360
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE

)

-- #####################################################################
--                              INPUT
--                            Functional
--              df_dwh_fstaebecam // dwh_fstaebecam
-- #####################################################################

CREATE TEMPORARY TABLE df_dwh_fstaebecam AS (

    SELECT 
        codpais,
        aniocampana,
        codebelista,
        constancia,
        codterritorio,
        codnivelcae
    FROM dwh_fstaebecam
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE

)

-- #####################################################################
--                              INPUT
--                            Functional
--              df_dwh_dgeografiacampana // dwh_dgeografiacampana
-- #####################################################################

CREATE TEMPORARY TABLE df_dwh_dgeografiacampana AS (

    SELECT 
        codpais,
        aniocampana,
        codterritorio,
        codregion,
        codzona
    FROM dwh_dgeografiacampana
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE

)

-- #####################################################################
--                              INPUT
--                            FncAnalitico
--              df_prg_dnivelcae // prg_dnivelcae
-- #####################################################################

CREATE TEMPORARY TABLE df_prg_dnivelcae AS (

    SELECT 
        codpais,
        cod_nive_cae,
        des_nive_cae
    FROM prg_dnivelcae
    WHERE codpais = $V_CODPAIS

)

-- #####################################################################
--                              INPUT
--                            Functional
--              df_tmp_plan_res_cache // df_tmp_plan_res_cache !!!(tabla generada en ETL_4)!!!
-- #####################################################################

CREATE TEMPORARY TABLE df_tmp_plan_res_cache AS (

    SELECT *
    FROM df_tmp_plan_res_cache
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE

)

------------------ INICIO df_grupocompra_revista ------------------
CREATE TEMPORARY TABLE df_grupocompra_revista AS (

    SELECT 
        codpais, 
        aniocampana, 
        codebelista,
        MAX(CASE WHEN medioventa = 'REVISTA' THEN '1' ELSE '0' END) AS revista,
        MAX(CASE WHEN medioventa = 'WEB' THEN '1' ELSE '0' END) AS digital,
        MAX(CASE WHEN medioventa = 'CATALOGO' THEN '1' ELSE '0' END) AS catalogo
    FROM df_tmp_plan_res_cache
    WHERE flagresultados = TRUE
    GROUP BY codpais, aniocampana, codebelista

)

------------------ INICIO df_grupocompra ------------------
CREATE TEMPORARY TABLE df_grupocompra AS (

    SELECT *,
    CASE WHEN revista = '0' AND digital = '1' THEN 'Digital'
         WHEN revista = '1' AND digital = '1' THEN 'Fisico + Digital'
         WHEN digital = '0' AND (revista = '1' OR catalogo = '1') THEN 'No Digital'
         ELSE NULL 
    END AS grupocompra
    FROM df_grupocompra_revista

)

-- #####################################################################
--                              INPUT
--                            DomAnalyticsc
--              df_consultora_pais_cluster // sgd_segmentodigital
-- #####################################################################

CREATE TEMPORARY TABLE df_consultora_pais_cluster AS (

    SELECT 
        codpais, 
        aniocampana AS campanaobjetivo, 
        codebelista, 
        segmento_digital AS perfildigital
    FROM sgd_segmentodigital
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE

)

------------------ INICIO df_tmp_atributos ------------------
CREATE TEMPORARY TABLE df_tmp_atributos AS (

    SELECT 
        dc.cod_pais AS codpais,
        dc.cod_aniocampana AS aniocampana,
        dc.cod_consultora AS codebelista,
        dc.bnd_activa AS flagactiva,
        dc.bnd_paso_pedido AS flagpasopedido,
        dc.des_status_comercial AS desstatuscomercial,
        dc.des_comportamiento AS descomportamiento,
        dc.bnd_ip_unicozona AS bndipunicozona,
        dc.cod_seccion AS codseccion,
        df.constancia,
        clu.perfildigital,
        dd.codregion,
        dd.codzona,
        pd.des_nive_cae AS segmentacioncaminobrillante,
        gc.grupocompra AS grupocompraganama

    FROM df_det_consultora360 dc
        LEFT JOIN df_dwh_fstaebecam df 
        ON dc.cod_pais = df.codpais 
            AND dc.cod_aniocampana = df.aniocampana 
            AND dc.cod_consultora = df.codebelista
            LEFT JOIN df_dwh_dgeografiacampana dd 
            ON dc.cod_pais = dd.codpais 
                AND dc.cod_aniocampana = dd.aniocampana 
                AND df.codterritorio = dd.codterritorio
                LEFT JOIN df_prg_dnivelcae pd 
                ON dc.cod_pais = pd.codpais 
                    AND df.codnivelcae = pd.cod_nive_cae
                    LEFT JOIN df_grupocompra gc 
                    ON dc.cod_pais = gc.codpais 
                        AND dc.cod_aniocampana = gc.aniocampana 
                        AND dc.cod_consultora = gc.codebelista
                        LEFT JOIN df_consultora_pais_cluster clu 
                        ON clu.codpais = dc.cod_pais 
                            AND clu.campanaobjetivo = dc.cod_aniocampana 
                            AND clu.codebelista = dc.cod_consultora

)

------------------ INICIO df_source ------------------
CREATE TEMPORARY TABLE df_source AS (

    SELECT
        tmp.codpais,
        tmp.aniocampana,
        tmp.codebelista ,
        tmp.cuc ,
        tmp.precionormalunitmn ,
        tmp.precioofertaunitmn ,
        tmp.unidades ,
        tmp.codventa ,
        tmp.codsap,
        tmp.desmarca ,
        tmp.desclase,
        CASE WHEN tmp.desunidadnegocio = '' THEN NULL ELSE tmp.desunidadnegocio END AS desunidadnegocio,
        CASE WHEN tmp.destipo = '' THEN NULL ELSE tmp.destipo END AS destipo,
        CASE WHEN tmp.desproducto = '' THEN NULL ELSE tmp.desproducto END AS desproducto,
        CASE WHEN tmp.descripcuc = '' THEN NULL ELSE tmp.descripcuc END AS descripcuc,
        CASE WHEN tmp.segmentorollingplan = '' THEN NULL ELSE tmp.segmentorollingplan END AS segmentorollingplan,
        CASE WHEN tmp.grupocompraplan = '' THEN NULL ELSE tmp.grupocompraplan END AS grupocompraplan,
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
        atr.flagactiva,
        atr.flagpasopedido,
        CASE WHEN atr.desstatuscomercial = '' THEN NULL ELSE atr.desstatuscomercial END AS desstatuscomercial,
        CASE WHEN atr.descomportamiento = '' THEN NULL ELSE atr.descomportamiento END AS descomportamiento,
        NVL (atr.bndipunicozona, 0) AS flagipunicozona,
        atr.codseccion,
        atr.constancia,
        atr.perfildigital,
        atr.codregion,
        atr.codzona,
        CASE WHEN atr.segmentacioncaminobrillante = '' THEN NULL ELSE atr.segmentacioncaminobrillante END AS segmentacioncaminobrillante,
        CASE WHEN atr.grupocompraganamas = '' THEN NULL ELSE atr.grupocompraganamas END AS grupocompraganamas,
        tmp.flagperfilx,
        CASE WHEN tmp.inputorigen IN('COP','MDG') THEN tmp.idoferta ELSE NULL END AS idoferta,
        tmp.inputorigen,
        tmp.idofertaorigen,
        tmp.reactioncandidateid,
        tmp.createddatesimulationreaction,
        tmp.oddcandidateid,
        tmp.cucpadre,
        tmp.costounitmn,
        CASE WHEN tmp.formatoplan = '' THEN NULL ELSE tmp.formatoplan END AS formatoplan,
        tmp.precionormalsetmn,
        tmp.precioofertasetmn,
        tmp.porcdescuentoset,
        tmp.costosetmn,
        tmp.flagmaterialganancia,
        tmp.flagcompuestavariable,
        tmp.factorcuadre,
        tmp.codventapadre,
        tmp.fmulticlase,
        tmp.descategoriaoferta AS claseoferta,
        tmp.fmultimarca,
        tmp.desmarcaoferta AS marcaoferta,
        CASE WHEN tmp.formatooferta = '' THEN NULL ELSE tmp.formatooferta END AS formatooferta,
        CASE WHEN tmp.descripcionoferta = '' THEN NULL ELSE tmp.descripcionoferta END AS descripcionoferta,
        tmp.unidadesset,
        tmp.comentario,
        CASE WHEN tmp.flagperfilx = true and tmp.orden is null then cx.orden ELSE tmp.orden END AS orden,
        tmp.mindiaodd,
        tmp.maxdiaodd,
        tmp.diasodd,
        tmp.flgexposprior,
        CASE WHEN tmp.procedencia = 'RESULTADOS' THEN NULL ELSE tmp.procedencia END AS procedencia,
        tmp.tactica
        ----------------------
        '&{P_IDCARGA}'  AS IDCARGA, ---- ??????
        '&{P_UUID_PROCESS}' AS UUIDFILE ---- ??????
        --                          ????? FALTA FECPROCESO ???
        ------------------------
    FROM df_tmp_temporal_5 tmp
        LEFT JOIN df_edp_asignacion_id_consultora_x cx 
        ON tmp.codpais = cx.codpais 
            AND tmp.aniocampana = cx.aniocampana
            AND tmp.idoferta = cx.idoferta
            LEFT JOIN df_tmp_atributos atr 
            ON tmp.codpais = atr.codpais 
                AND tmp.aniocampana = atr.aniocampana 
                AND tmp.codebelista = atr.codebelista
    WHERE 
        tmp.cuc IS NOT NULL 
        AND tmp.codventa IS NOT NULL
)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL_6 ----------------------------
----------------------- edp_consolidadoconsultora ----------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================

CREATE TEMPORARY TABLE edp_consolidadoconsultora AS (

    SELECT 
        *, 
        'NOT' AS codigopalanca
    FROM df_source
)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL_6 ----------------------------
----------------------- edp_consolidadoconsultora ----------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================