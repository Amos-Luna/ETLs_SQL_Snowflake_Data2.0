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
--  CREACION DE TABLAS TEMPORALES ===>>> REVISAR QUE SE NECESITA
--  ----------------------------------------
-- =========================================




-- =========================================
-- -----------------------------------------
--  INICIO DE LÓGICA DE NEGOCIO
--  ----------------------------------------
-- =========================================

-- #####################################################################
--                              INPUT 1 
--      eda_asignacion_consultora_id / edpda_asignacionidconsultora (tabla generada en ETL_3)
-- #####################################################################

CREATE TEMPORARY TABLE df_edp_asignacion_id_consultora AS ( -->>>>>>>>>> VARIABLE A USAR EN SPARK

    CREATE TEMPORARY TABLE edpda_asignacionidconsultora_aux AS (

        SELECT DISTINCT 
            codpais, 
            aniocampana, 
            codebelista, 
            idoferta, 
            orden, 
            TRIM(tactica) AS tactica, 
            inputorigen, 
            idofertaorigen,
            reactioncandidateid, 
            createddatesimulationreaction, 
            oddcandidateid, 
            dia, 
            procedencia, 
            flgexposprior,
            ROW_NUMBER() OVER (
                PARTITION BY codpais,
                            aniocampana,
                            codebelista, 
                            idoferta, 
                            oddcandidateid, 
                            dia
                ORDER BY inputorigen DESC NULLS LAST,
                        orden ASC NULLS LAST,
                        flgexposprior DESC NULLS LAST,
                        idofertaorigen ASC NULLS LAST
            ) AS fila

        FROM edpda_asignacionidconsultora ---- SE NECESITA CREAR ???

        WHERE codpais = $V_CODPAIS
            AND UUIDFILE = $P_UUIDFILE
    )
    
    SELECT *
    FROM edpda_asignacionidconsultora_aux
    WHERE fila = 1;
)

-- #####################################################################
--                              INPUT 2 
--      eda_planeacion_ofertas /// edpda_planeacionoferta (tabla generada en ETL_2)
-- #####################################################################

CREATE TEMPORARY TABLE df_edp_planeacion_oferta AS ( -->>>>>>>>>> VARIABLE A USAR EN SPARK

    CREATE TEMPORARY TABLE edpda_planeacionoferta_aux AS (

        SELECT DISTINCT 
            codpais, 
            aniocampana, 
            aniocampanaplan, 
            idoferta, 
            cuc, 
            cucpadre, 
            precionormalunitmn, 
            precioofertaunitmn,
            costounitmn, 
            unidades,
            reactioncandidateid, 
            createddatesimulationreaction, 
            oddcandidateid, 
            idofertaorigen, 
            inputorigen, 
            TRIM(tactica) AS tactica, 
            formatooferta, 
            comentario, 
            codventa, 
            codsap, 
            precionormalsetmn, 
            precioofertasetmn, 
            porcdescuentoset, 
            costosetmn, 
            flagmaterialganancia, 
            flagcompuestavariable, 
            factorcuadre, 
            codventapadre, 
            desmarca, 
            desclase, 
            desnegocio AS desunidadnegocio,
            destipo, 
            dessubcategoria, 
            destiposolo, 
            deslinea, 
            desproducto, 
            descripcuc, 
            fmulticlase, 
            claseplan, 
            fmultimarca, 
            marcaplan, 
            formatoplan,
            descripcion, 
            unidadesset, 
            desmarcaoferta, 
            descategoriaoferta, 
            CASE WHEN inputorigen = 'ARP' THEN 0 ELSE 1 END AS peso,
            ROW_NUMBER() OVER (
                PARTITION BY codpais, 
                            aniocampana, 
                            codventa
                ORDER BY peso DESC NULLS LAST,
                        flagmaterialganancia DESC NULLS LAST,
                        idofertaorigen ASC NULLS LAST
            ) AS fila

        FROM edpda_planeacionoferta

        WHERE codpais = $V_CODPAIS
            AND UUIDFILE = $P_UUIDFILE
            AND (idoferta IS NOT NULL OR codventa IS NOT NULL)
    )

    SELECT *
    FROM edpda_planeacionoferta_aux
    WHERE fila = 1;
)

------------------ INICIO df_tmp_asign_planeacion_digital ------------------
CREATE TEMPORARY TABLE df_tmp_asign_planeacion_digital AS (
    SELECT
        asi.codpais,
        asi.aniocampana,
        asi.codebelista,
        asi.idoferta,
        asi.orden AS orden,
        asi.tactica,
        asi.inputorigen,
        asi.idofertaorigen,
        asi.reactioncandidateid,
        asi.createddatesimulationreaction,
        asi.oddcandidateid,
        asi.procedencia,
        asi.flgexposprior AS flgexposprior,
        epo.aniocampanaplan,
        epo.cuc,
        epo.cucpadre,
        epo.precionormalunitmn,
        epo.precioofertaunitmn,
        epo.costounitmn,
        epo.unidades,
        epo.formatooferta,
        epo.comentario,
        epo.codventa,
        epo.codsap,
        epo.precionormalsetmn,
        epo.precioofertasetmn,
        epo.porcdescuentoset,
        epo.costosetmn,
        epo.flagmaterialganancia,
        epo.flagcompuestavariable,
        epo.factorcuadre,
        epo.codventapadre,
        epo.desclase,
        epo.desunidadnegocio,
        epo.destipo,
        epo.dessubcategoria,
        epo.destiposolo,
        epo.deslinea,
        epo.desproducto,
        epo.descripcuc,
        epo.fmulticlase,
        epo.claseplan,
        epo.fmultimarca,
        epo.marcaplan,
        epo.formatoplan,
        epo.descripcion,
        epo.unidadesset,
        epo.desmarcaoferta,
        epo.descategoriaoferta,
        CAST(NULL AS INT) AS mindiaodd,
        CAST(NULL AS INT) AS maxdiaodd,
        CAST(NULL AS INT) AS diasodd
    FROM df_edp_asignacion_id_consultora asi
    LEFT JOIN df_edp_planeacion_oferta epo 
        ON asi.codpais = epo.codpais 
        AND asi.aniocampana = epo.aniocampana 
        AND asi.idoferta = epo.idoferta
)

------------------ INICIO df_moda ------------------
CREATE TEMPORARY TABLE df_moda AS (

    CREATE TEMPORARY TABLE df_moda_pre AS (

        SELECT codpais, 
                aniocampana, 
                codebelista, 
                idoferta, 
                orden, 
                COUNT(codpais) AS cnt
        FROM df_edp_asignacion_id_consultora
        GROUP BY codpais, aniocampana, codebelista, idoferta, orden
    )

    SELECT *
    FROM (
        SELECT *,
                ROW_NUMBER() OVER (PARTITION BY codpais, 
                                                aniocampana, 
                                                codebelista, 
                                                idoferta 
                                    ORDER BY cnt DESC NULLS LAST) AS sqnum
        FROM df_moda_pre
    ) t
    WHERE sqnum = 1;
)

------------------ INICIO df_tmp_info_odd_distinct ------------------
CREATE TEMPORARY TABLE df_tmp_info_odd_distinct AS (

    CREATE TEMPORARY TABLE df_tmp_info_odd AS (

        SELECT
            teaci.codpais as codpais,
            teaci.aniocampana as aniocampana,
            teaci.codebelista as codebelista,
            teaci.idoferta as idoferta,
            moda.orden AS ordenmodaodd,
            teaci.oddcandidateid as oddcandidateid,
            MIN(teaci.dia) AS mindiaodd,
            MAX(teaci.dia) AS maxdiaodd,
            COUNT(DISTINCT teaci.dia) AS diasodd
        FROM
            df_edp_asignacion_id_consultora teaci
            INNER JOIN df_moda moda 
                ON teaci.codpais = moda.codpais
                    AND teaci.aniocampana = moda.aniocampana
                    AND teaci.codebelista = moda.codebelista
                    AND teaci.idoferta = moda.idoferta
        GROUP BY
            teaci.codpais,
            teaci.aniocampana,
            teaci.codebelista,
            teaci.idoferta,
            moda.orden,
            teaci.oddcandidateid
    )

    SELECT
        codpais,
        aniocampana,
        MIN(mindiaodd) AS mindiaodd,
        MAX(maxdiaodd) AS maxdiaodd,
        MAX(diasodd) AS diasodd,
        MAX(ordenmodaodd) AS ordenmodaodd,
        codebelista,
        idoferta,
        oddcandidateid
    FROM df_tmp_info_odd
    GROUP BY
        codpais,
        aniocampana,
        codebelista,
        idoferta,
        oddcandidateid
)

------------------ INICIO df_tmp_asign_planeacion_digital_updated ------------------
CREATE TEMPORARY TABLE df_tmp_asign_planeacion_digital_updated AS (

    CREATE TEMPORARY TABLE df_tmp_asign_planeacion_digital_updated_rec AS (
        SELECT *,
                ROW_NUMBER() OVER (
                PARTITION BY codpais, 
                                aniocampana,
                                codebelista, 
                                codventa,
                                codsap
                ORDER BY inputorigen DESC NULLS LAST, 
                            idofertaorigen ASC NULLS LAST
                ) AS fila
                
        FROM df_tmp_asign_planeacion_digital tmp
        LEFT JOIN df_tmp_info_odd_distinct t2
                ON tmp.codpais = t2.codpais
                AND tmp.aniocampana = t2.aniocampana
                AND tmp.codebelista = t2.codebelista
                AND tmp.idoferta = t2.idoferta
                AND tmp.oddcandidateid = t2.oddcandidateid
    )
    SELECT codpais,
        aniocampana,
        codebelista,
        idoferta,
        COALESCE(t2.ordenmodaodd, orden) AS orden,
        tactica,
        inputorigen,
        idofertaorigen,
        reactioncandidateid,
        createddatesimulationreaction,
        oddcandidateid,
        procedencia,
        flgexposprior,
        aniocampanaplan,
        cuc,
        cucpadre,
        precionormalunitmn,
        precioofertaunitmn,
        costounitmn,
        unidades,
        formatooferta,
        comentario,
        codventa,
        codsap,
        precionormalsetmn,
        precioofertasetmn,
        porcdescuentoset,
        costosetmn,
        flagmaterialganancia,
        flagcompuestavariable,
        factorcuadre,
        codventapadre,
        desclase,
        desunidadnegocio,
        destipo,
        dessubcategoria,
        destiposolo,
        deslinea,
        desproducto,
        descripcuc,
        fmulticlase,
        claseplan,
        fmultimarca,
        marcaplan,
        formatoplan,
        descripcion,
        unidadesset,
        desmarcaoferta,
        descategoriaoferta,
        t2.mindiaodd AS mindiaodd,
        t2.maxdiaodd AS maxdiaodd,
        t2.diasodd AS diasodd
    FROM df_tmp_asign_planeacion_digital_updated_rec
    WHERE fila = 1
)

-- #####################################################################
--                              INPUT 3 
--     df_corp_det_planit_digital  /// corp_det_planit_digital (tabla generada en ETL_7 - Enmanuel)
-- #####################################################################

CREATE TEMPORARY TABLE df_corp_det_planit_digital AS (

    SELECT 
        codpais,
        trim(aniocampana) AS aniocampana,
        CONCAT(fechaproceso, ' 16:00:00') AS fechaproceso,
        dispositivoajustado,
        codregion,
        codzona,
        trim(codebelista) AS codebelista,
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
        trim(codventa) AS codventa,
        trim(palancaajustado) AS palancaajustado,
        desmedioventaajustado,
        grupocompraganamas,
        null AS desclaseajustado,
        formatoplan AS formato,
        codestrategia,
        numoferta AS nrooferta,
        cantidadpedidospais,
        trim(aniocampanaref) AS aniocampanaref,
        flagventacomportamiento,
        desespacio,
        despopup,
        codseccion,
        constancia,
        segmentacionrolling AS descomportamiento,
        codsap
    FROM corp_det_planit_digital
    WHERE 
        codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE 
        AND flagventacomportamiento = 1 
        AND realuuvendidas > 0;

)

-- #####################################################################
--                              INPUT 4 
--    df_dwh_fvtaproebecam   /// dwh_fvtaproebecam
-- #####################################################################

CREATE TEMPORARY TABLE df_dwh_fvtaproebecam AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaref, 
        codebelista, 
        codcanalventa, 
        codventa, 
        codtipooferta, 
        realuufaltantes, 
        realvtamnfaltneto, 
        codsap, 
        ROW_NUMBER() OVER(PARTITION BY codpais, 
                                    aniocampana,
                                    aniocampanaref, 
                                    codebelista, 
                                    codcanalventa, 
                                    codventa, 
                                    codtipooferta, 
                                    codsap 
                            ORDER BY realuufaltantes DESC NULLS LAST) AS fila
    FROM dwh_fvtaproebecam
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE 
        AND realuuvendidas > 0
    HAVING fila = 1

)

-- #####################################################################
--                              INPUT 5 
--                  df_dwh_dproducto  /// dwh_dproducto
-- #####################################################################

CREATE TEMPORARY TABLE df_dwh_dproducto AS (

    SELECT 
        codsap, 
        desclase, 
        destipo, 
        COALESCE(descripcuc, desproducto) AS descripcuc, 
        desproducto, 
        COALESCE(codcuc, cuc) AS codcuc, 
        desmarca
    FROM dwh_dproducto

)

-- #####################################################################
--                              INPUT 6 
--           df_dwh_dmatrizcampana  /// dwh_dmatrizcampana
-- #####################################################################

CREATE TEMPORARY TABLE df_dwh_dproducto AS (

    SELECT +
        codpais, 
        aniocampana, 
        codventa, 
        nropagina, 
        precionormalmn, 
        preciooferta, 
        codcanalventa, 
        codtipooferta, 
        codcatalogo, 
        codestrategia, 
        numoferta, 
        codsap, 
        COALESCE(flagreaccion, 1) AS flagreaccion_join, 
        indpadre
    FROM dwh_dmatrizcampana
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE

)

------------------ INICIO df_tmp_resultados_rec_group ------------------
CREATE TEMPORARY TABLE df_tmp_resultados_rec_group AS (

    SELECT
        cdpd.codpais,
        cdpd.aniocampana,
        cdpd.aniocampanaref,
        cdpd.fechaproceso,
        cdpd.dispositivoajustado,
        cdpd.codebelista,
        cdpd.desunidadnegocio,
        cdpd.desclaseajustado,
        cdpd.realvtamnneto,
        cdpd.realvtadolneto,
        cdpd.realuuvendidas,
        cdpd.realcostomnvendidas,
        cdpd.realcostodolvendidas,
        cdpd.desproductocuc,
        cdpd.cuc,
        cdpd.codventa,
        cdpd.palancaajustado,
        cdpd.desespacio,
        cdpd.despopup,
        cdpd.desmedioventaajustado,
        cdpd.formato,
        cdpd.codestrategia,
        cdpd.nrooferta,
        cdpd.cantidadpedidospais,
        cdpd.codseccion,
        cdpd.codregion,
        cdpd.codzona,
        cdpd.constancia,
        cdpd.descomportamiento,
        cdpd.segmentacioncaminobrillante,
        df.codtipooferta AS codtipooferta ,
        df.codcanalventa AS codcanalventa,
        df.realuufaltantes AS realuufaltantes,
        df.realvtamnfaltneto AS realvtamnfaltneto,
        df.codsap AS codsap
    FROM df_corp_det_planit_digital cdpd
        LEFT JOIN df_dwh_fvtaproebecam df ON
        cdpd.codpais = df.codpais AND
        cdpd.aniocampana = df.aniocampana AND
        cdpd.aniocampanaref = df.aniocampanaref AND
        cdpd.codebelista = df.codebelista AND
        cdpd.codsap = df.codsap AND
        cdpd.codventa = df.codventa

)

------------------ INICIO df_tmp_resultados_rec ------------------
CREATE TEMPORARY TABLE df_tmp_resultados_rec AS (

    SELECT
        cdpd.codpais,
        cdpd.aniocampana,
        cdpd.aniocampanaref,
        cdpd.fechaproceso,
        cdpd.dispositivoajustado,
        cdpd.codebelista,
        cdpd.desunidadnegocio,
        cdpd.desclaseajustado,
        cdpd.realvtamnneto,
        cdpd.realvtadolneto,
        cdpd.realuuvendidas,
        cdpd.realcostomnvendidas,
        cdpd.realcostodolvendidas,
        cdpd.cuc,
        cdpd.codventa,
        cdpd.palancaajustado,
        cdpd.desespacio,
        cdpd.despopup,
        cdpd.desmedioventaajustado,
        cdpd.formato,
        cdpd.codestrategia,
        cdpd.nrooferta,
        cdpd.cantidadpedidospais,
        cdpd.codseccion,
        cdpd.codregion,
        cdpd.codzona,
        cdpd.constancia,
        cdpd.descomportamiento,
        cdpd.segmentacioncaminobrillante,
        cdpd.codtipooferta AS codtipooferta,
        cdpd.realuufaltantes,
        cdpd.realvtamnfaltneto,
        COALESCE(cdpd.codsap, dd2.codsap) AS codsap,
        dd.nropagina,
        dd.precionormalmn,
        dd.preciooferta,
        dd2.codventa AS codventapadre,
        cdpd.cuc AS cucpadre
    FROM df_tmp_resultados_rec_group cdpd
    LEFT JOIN df_dwh_dmatrizcampana dd
    ON cdpd.codpais = dd.codpais
    AND cdpd.aniocampana = dd.aniocampana
    AND cdpd.codventa = dd.codventa
    AND cdpd.codtipooferta = dd.codtipooferta
    AND cdpd.codcanalventa = dd.codcanalventa
    AND cdpd.codsap = dd.codsap
        LEFT JOIN df_dwh_dmatrizcampana dd2
        ON dd.codpais = dd2.codpais
        AND dd.aniocampana = dd2.aniocampana
        AND dd.codestrategia = dd2.codestrategia
        AND dd.numoferta = dd2.numoferta
        AND dd.codventa = dd2.codventa
        AND dd2.indpadre = '1'


)

------------------ INICIO df_tmp_resultados_rec ------------------
CREATE TEMPORARY TABLE df_tmp_resultados AS (

    SELECT codpais,
        aniocampana,
        aniocampanaref,
        fechaproceso,
        dispositivoajustado,
        codebelista,
        desunidadnegocio,
        desclaseajustado,
        realvtamnneto,
        realvtadolneto,
        realuuvendidas,
        realcostomnvendidas,
        realcostodolvendidas,
        cuc,
        codventa,
        CASE WHEN upper(trim(palancaajustado)) = 'DIGITADO' THEN 'DIG'
                WHEN upper(trim(palancaajustado)) = 'LANZAMIENTOS' THEN 'LAN'
                WHEN upper(trim(translate(palancaajustado,'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU'))) = 'OFERTA DEL DIA' THEN 'ODD'
                WHEN upper(trim(palancaajustado)) = 'SHOWROOM' THEN 'SR'
                WHEN upper(trim(palancaajustado)) = 'OFERTAS PARA TI' THEN 'OPT'
                WHEN upper(trim(palancaajustado)) = 'OFERTA FINAL' THEN 'OF'
                WHEN upper(trim(palancaajustado)) = 'GANADORAS' THEN 'LMG'
                WHEN upper(trim(palancaajustado)) = 'ARMA TU PACK' THEN 'ATP'
                WHEN upper(trim(palancaajustado)) = 'GND' THEN 'LMG'
                WHEN upper(trim(translate(palancaajustado,'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU'))) = 'LIQUIDACION' THEN 'LIQ'
                WHEN upper(trim(palancaajustado)) = 'PRODUCTO SUGERIDO' THEN 'DIG'
                WHEN upper(trim(palancaajustado)) = '' OR palancaajustado is null THEN 'DIG'
                ELSE upper(trim(translate(palancaajustado,'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU'))) END AS palancaajustado,
        desespacio,
        despopup,
        upper(trim(desmedioventaajustado)) as desmedioventaajustado,
        formato,
        codestrategia,
        nrooferta,
        cantidadpedidospais,
        codseccion,
        codregion,
        codzona,
        constancia,
        descomportamiento,
        segmentacioncaminobrillante,
        codtipooferta,
        max(realuufaltantes) as realuufaltantes,
        max(realvtamnfaltneto) as realvtamnfaltneto,
        max(codsap) as codsap,
        max(nropagina) as nropagina,
        max(precionormalmn) as precionormalmn,
        max(preciooferta) as preciooferta,
        codventapadre,
        cucpadre
    FROM df_tmp_resultados_rec
    GROUP BY codpais,
            aniocampana,
            aniocampanaref,
            fechaproceso,
            dispositivoajustado,
            codebelista,
            desunidadnegocio,
            desclaseajustado,
            realvtamnneto,
            realvtadolneto,
            realuuvendidas,
            realcostomnvendidas,
            realcostodolvendidas,
            cuc,
            codventa,
            palancaajustado,
            desespacio,
            despopup,
            desmedioventaajustado,
            formato,
            codestrategia,
            nrooferta,
            cantidadpedidospais,
            codseccion,
            codregion,
            codzona,
            constancia,
            descomportamiento,
            segmentacioncaminobrillante,
            codventapadre,
            cucpadre

)

------------------ INICIO df_tmp_plan_res_inicial_recpre ------------------
CREATE TEMPORARY TABLE df_tmp_plan_res_inicial_recpre AS (

    SELECT
        COALESCE(plan.codpais, res.codpais) AS codpais,
        COALESCE(plan.aniocampana, res.aniocampana) AS aniocampana,
        COALESCE(plan.codebelista, res.codebelista) AS codebelista,
        plan.idoferta,
        plan.orden,
        plan.tactica,
        plan.inputorigen,
        NULL AS idofertaorigen,
        plan.reactioncandidateid,
        plan.createddatesimulationreaction,
        plan.oddcandidateid,
        plan.procedencia,
        plan.flgexposprior,
        plan.aniocampanaplan,
        COALESCE(plan.cuc, res.cuc) AS cuc,
        plan.cucpadre,
        COALESCE(plan.precionormalunitmn, res.precionormalmn) AS precionormalunitmn,
        COALESCE(plan.precioofertaunitmn, res.preciooferta) AS precioofertaunitmn,
        plan.costounitmn,
        plan.unidades,
        plan.formatooferta,
        plan.comentario,
        COALESCE(plan.codventa, res.codventa) AS codventa,
        COALESCE(plan.codsap, res.codsap) AS codsap,
        plan.precionormalsetmn,
        plan.precioofertasetmn,
        plan.porcdescuentoset,
        plan.costosetmn,
        plan.flagmaterialganancia,
        plan.flagcompuestavariable,
        plan.factorcuadre,
        plan.codventapadre,
        TRIM(plan.desclase) AS desclase,
        TRIM(COALESCE(plan.desunidadnegocio, res.desunidadnegocio)) AS desunidadnegocio,
        plan.dessubcategoria,
        plan.destiposolo,
        plan.deslinea,
        plan.fmulticlase,
        plan.claseplan,
        plan.fmultimarca,
        plan.marcaplan,
        plan.formatoplan,
        plan.descripcion,
        plan.unidadesset,
        plan.desmarcaoferta,
        plan.descategoriaoferta,
        plan.mindiaodd,
        plan.maxdiaodd,
        plan.diasodd,
        res.aniocampanaref,
        res.fechaproceso,
        res.dispositivoajustado,
        res.desclaseajustado,
        res.realvtamnneto,
        res.realvtadolneto,
        res.realuuvendidas,
        res.realcostomnvendidas,
        res.realcostodolvendidas,
        res.desespacio,
        res.despopup,
        res.desmedioventaajustado,
        res.formato,
        res.codestrategia,
        res.nrooferta,
        res.cantidadpedidospais,
        res.codtipooferta,
        res.realuufaltantes,
        res.realvtamnfaltneto,
        res.nropagina,
        res.codseccion AS codseccionres,
        res.codregion AS codregionres,
        res.codzona AS codzonares,
        res.constancia AS constanciares,
        res.descomportamiento AS descomportamientores,
        res.segmentacioncaminobrillante AS segmentacioncaminobrillanteres
        FROM df_tmp_asign_planeacion_digital_updated plan
        FULL OUTER JOIN df_tmp_resultados res
            ON plan.codpais = res.codpais
            AND plan.aniocampana = res.aniocampana
            AND plan.codebelista = res.codebelista
            AND plan.codventa = res.codventa
            AND plan.codsap = res.codsap

)

------------------ INICIO df_tmp_plan_res_inicial ------------------
CREATE TEMPORARY TABLE df_tmp_plan_res_inicial AS (

    SELECT
        plan.codpais,
        plan.aniocampana,
        plan.codebelista,
        plan.idoferta,
        plan.orden,
        plan.tactica,
        plan.inputorigen,
        plan.idofertaorigen,
        plan.reactioncandidateid,
        plan.createddatesimulationreaction,
        plan.oddcandidateid,
        plan.procedencia,
        plan.flgexposprior,
        plan.aniocampanaplan,
        plan.cuc,
        plan.cucpadre,
        plan.precionormalunitmn,
        plan.precioofertaunitmn,
        plan.costounitmn,
        plan.unidades,
        plan.formatooferta,
        plan.comentario,
        plan.codventa,
        plan.codsap,
        plan.precionormalsetmn,
        plan.precioofertasetmn,
        plan.porcdescuentoset,
        plan.costosetmn,
        plan.flagmaterialganancia,
        plan.flagcompuestavariable,
        plan.factorcuadre,
        plan.codventapadre,
        dd.desmarca,
        dd.desclase,
        plan.desunidadnegocio,
        dd.destipo,
        plan.dessubcategoria,
        plan.destiposolo,
        plan.deslinea,
        dd.desproducto,
        dd.descripcuc,
        plan.fmulticlase,
        plan.claseplan,
        plan.fmultimarca,
        COALESCE(plan.marcaplan, dd.desmarca) AS marcaplan,
        plan.formatoplan,
        plan.descripcion,
        plan.unidadesset,
        plan.desmarcaoferta,
        plan.descategoriaoferta,
        plan.mindiaodd,
        plan.maxdiaodd,
        plan.diasodd,
        plan.aniocampanaref,
        plan.fechaproceso,
        plan.dispositivoajustado,
        dd.desmarca AS marcares,
        plan.desclaseajustado,
        plan.realvtamnneto,
        plan.realvtadolneto,
        plan.realuuvendidas,
        plan.realcostomnvendidas,
        plan.realcostodolvendidas,
        plan.desespacio,
        plan.despopup,
        plan.desmedioventaajustado,
        plan.formato,
        plan.codestrategia,
        plan.nrooferta,
        plan.cantidadpedidospais,
        plan.codtipooferta,
        plan.realuufaltantes,
        plan.realvtamnfaltneto,
        plan.nropagina,
        plan.codseccionres,
        plan.codregionres,
        plan.codzonares,
        plan.constanciares,
        plan.descomportamientores,
        plan.segmentacioncaminobrillanteres,
        CASE WHEN plan.reactioncandidateid IS NOT NULL THEN 0 ELSE 1 
        END AS reactioncandidateid_join
    FROM df_tmp_plan_res_inicial_recpre plan
    LEFT JOIN df_dwh_dproducto dd 
    ON plan.codsap = dd.codsap

)

------------------ INICIO df_dwh_dmatrizcampana_g ------------------
CREATE TEMPORARY TABLE df_dwh_dmatrizcampana_g AS (

    SELECT DISTINCT
        codpais,
        aniocampana,
        codventa,
        flagreaccion_join,
        codsap,
        codcatalogo,
        codestrategia,
        numoferta,
        codtipooferta
    FROM df_dwh_dmatrizcampana

)

------------------ INICIO df_planeacion_sin_atributos ------------------
CREATE TEMPORARY TABLE df_planeacion_sin_atributos AS (

    SELECT        
        pr.codpais,
        pr.aniocampana,
        pr.codebelista,
        pr.idoferta,
        pr.orden,
        pr.tactica,
        pr.inputorigen,
        pr.createddatesimulationreaction,
        pr.oddcandidateid,
        pr.procedencia,
        pr.flgexposprior,
        pr.aniocampanaplan,
        pr.cuc,
        pr.cucpadre,
        pr.precionormalunitmn,
        pr.precioofertaunitmn,
        pr.costounitmn,
        pr.unidades,
        pr.formatooferta,
        pr.comentario,
        pr.codventa,
        pr.codsap,
        pr.precionormalsetmn,
        pr.precioofertasetmn,
        pr.porcdescuentoset,
        pr.costosetmn,
        pr.flagmaterialganancia,
        pr.flagcompuestavariable,
        pr.factorcuadre,
        pr.codventapadre,
        pr.desmarca,
        pr.desclase,
        pr.desunidadnegocio,
        pr.destipo,
        pr.dessubcategoria,
        pr.destiposolo,
        pr.deslinea,
        pr.desproducto,
        pr.descripcuc,
        pr.fmulticlase,
        pr.claseplan,
        pr.fmultimarca,
        pr.marcaplan,
        pr.formatoplan,
        pr.descripcion,
        pr.unidadesset,
        pr.desmarcaoferta,
        pr.descategoriaoferta, 
        pr.mindiaodd,
        pr.maxdiaodd,
        pr.diasodd,
        pr.aniocampanaref,
        pr.fechaproceso,
        pr.dispositivoajustado,
        pr.marcares,
        pr.desclaseajustado,
        pr.realvtamnneto,
        pr.realvtadolneto,
        pr.realuuvendidas,
        pr.realcostomnvendidas,
        pr.realcostodolvendidas,
        pr.desespacio,
        pr.despopup,
        pr.desmedioventaajustado,
        pr.formato,
        pr.codestrategia,
        pr.nrooferta,
        pr.cantidadpedidospais,
        pr.codtipooferta,
        pr.realuufaltantes,
        pr.realvtamnfaltneto,
        pr.nropagina,
        pr.codseccionres,
        pr.codregionres,
        pr.codzonares,
        pr.constanciares,
        pr.descomportamientores,
        pr.segmentacioncaminobrillanteres,
        dd.codcatalogo,
        dd.codestrategia AS codestrategiamatriz,
        dd.numoferta AS numofertamatriz
    FROM df_tmp_plan_res_inicial pr
    LEFT JOIN df_dwh_dmatrizcampana_g dd 
    ON pr.codpais = dd.codpais
        AND pr.aniocampana = dd.aniocampana
        AND pr.codventa = dd.codventa
        AND pr.codtipooferta = dd.codtipooferta
        AND pr.codsap = dd.codsap

)

------------------ INICIO df_tmp_plan_res ------------------
CREATE TEMPORARY TABLE df_tmp_plan_res AS (

    SELECT 
        t1.codpais,
        t1.aniocampana,
        t1.codebelista,
        t1.codseccionres AS codseccion,
        t1.constanciares AS constancia,
        t1.idoferta,
        t1.orden,
        t1.tactica,
        t1.inputorigen,
        t1.createddatesimulationreaction,
        t1.oddcandidateid,
        t1.mindiaodd,
        t1.maxdiaodd,
        t1.diasodd,
        t1.procedencia,
        t1.flgexposprior,
        t1.cuc,
        t1.cucpadre,
        t1.precionormalunitmn,
        t1.precioofertaunitmn,
        t1.costounitmn,
        CAST(t1.unidades AS INTEGER) AS unidades,
        t1.formatoplan,
        t1.codventa,
        t1.codsap,
        t1.precionormalsetmn,
        t1.precioofertasetmn,
        t1.porcdescuentoset,
        t1.costosetmn,
        COALESCE(t1.flagmaterialganancia, FALSE) AS flagmaterialganancia,
        COALESCE(t1.flagcompuestavariable, 0) AS flagcompuestavariable,
        COALESCE(t1.factorcuadre, 0) AS factorcuadre,
        t1.codventapadre,
        t1.desmarca AS desmarca,
        t1.desclase AS desclase,
        CASE 
            WHEN t1.desunidadnegocio IN ('ACCESORIOS', 'HOGAR', 'MODA') THEN 'M&A'
            WHEN t1.desunidadnegocio = 'APOYO' THEN 'APOYO'
            WHEN t1.desunidadnegocio = 'COSMETICOS' THEN 'COSM'
            ELSE t1.desunidadnegocio 
        END AS desunidadnegocio,
        t1.destipo AS destipo,
        t1.desproducto AS desproducto,
        t1.descripcuc AS descripcuc,
        t1.fmulticlase,
        t1.claseplan AS claseoferta,
        t1.fmultimarca,
        t1.marcaplan AS marcaoferta,
        t1.formatoplan AS formatooferta,
        t1.descripcion AS descripcionoferta,
        t1.unidadesset,
        t1.desmarcaoferta, 
        t1.descategoriaoferta,
        t1.comentario,
        t1.fechaproceso,
        t1.dispositivoajustado,
        t1.desespacio,
        t1.despopup,
        t1.realvtamnneto,
        t1.realvtadolneto,
        t1.realuuvendidas,
        t1.realcostomnvendidas,
        t1.realcostodolvendidas,
        COALESCE(t1.codestrategiamatriz, t1.codestrategia) AS codestrategia,
        COALESCE(t1.numofertamatriz, t1.nrooferta) AS nrooferta,
        t1.cantidadpedidospais,
        t1.codtipooferta,
        t1.realuufaltantes,
        t1.realvtamnfaltneto,
        t1.aniocampanaref,
        t1.segmentacioncaminobrillanteres,
        CASE
            WHEN COALESCE(t1.flagmaterialganancia, false) = true THEN true
            WHEN t1.codcatalogo IN ('44','45','46') AND t1.aniocampana >= 202108 THEN true
            WHEN CAST(t1.codtipooferta AS int) >= 200 AND CAST(t1.codtipooferta AS int) <= 228 THEN true
            WHEN t1.idoferta IS NULL THEN false
            ELSE true
        END AS flagofertadigital,
        CASE
            WHEN t1.codcatalogo IN ('44','45','46') AND t1.aniocampana >= 202108 THEN 'WEB'
            WHEN t1.codtipooferta IN ('001','002','003','025','026','027','029','030','031','032','035','037','038','041','046','048','049','050','055','060','064','065','100','105','107','108','113','115','123','125','127','128','130') THEN 'REVISTA'
            WHEN t1.codtipooferta IN ('004','005','006','007','008','009','010','011','012','013','014','015','016','017','018','019','020','024','033','034','036','039','040','042','043','044','047','051','052','053','106','111','114','116','117') THEN 'CATALOGO'
            WHEN t1.codtipooferta IN ('300','303','301','309','310','311','319','320','321','322','323','324','329','330','331','332','333') THEN 'CATALOGO'
            WHEN t1.codtipooferta IN ('401','402','409','411','412','419','421','422','423','424','425','429') THEN 'REVISTA'
            WHEN CAST(t1.codtipooferta AS int) >= 200 AND CAST(t1.codtipooferta AS int) <= 228 THEN 'WEB'
            WHEN t1.codtipooferta IS NULL THEN 'WEB'
            ELSE 'OTROS'
        END AS medioventa,
        CASE
            WHEN t1.realuuvendidas > 0 THEN TRUE
            ELSE false
        END AS flagresultados,
        CASE
            WHEN t1.codcatalogo IN ('44','45','46') AND t1.aniocampana >= 202108 AND CAST(t1.codtipooferta AS int) < 200 THEN TRUE
            ELSE FALSE
        END AS flagreplicarevista,
        FALSE AS flagofertafinal,
        FALSE AS flagprodsugerido,
        t1.codcatalogo
    FROM df_planeacion_sin_atributos t1
    WHERE medioventa IS NOT NULL;

)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL_4 ----------------------------
----------------------- df_tmp_plan_res_cache ----------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================

-- ------- HAY PARTICIONES EN LA LINEA 970 SCALA, REVISAR, IMPLICA ALGO ???  -----------

CREATE TEMPORARY TABLE df_tmp_plan_res_cache AS (

    SELECT
        codpais,
        aniocampana,
        codebelista,
        medioventa,
        codestrategia,
        nrooferta,
        desmarca,
        desclase,
        cuc,
        descripcuc,
        desproducto,
        unidades,
        precionormalunitmn,
        precioofertaunitmn,
        flagresultados
    FROM df_tmp_plan_res

)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL_4 ----------------------------
----------------------- df_tmp_plan_res_cache ----------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================


------------------ INICIO df_tmp_atributos_ofertas_fis ------------------
CREATE TEMPORARY TABLE df_tmp_atributos_ofertas_fis AS (

    SELECT 
        codestrategia, 
        nrooferta, 
        desmarca, 
        desclase, 
        cuc,
        COALESCE(desproducto, descripcuc) AS desproducto,
        SUM(unidades) AS unidades,
        MAX(precionormalunitmn) AS precionormalunitmn,
        MAX(precioofertaunitmn) AS precioofertaunitmn
    FROM df_tmp_plan_res_cache
    WHERE medioventa != 'WEB'
    GROUP BY codestrategia, nrooferta, desmarca, desclase, cuc, desproducto

)

------------------ INICIO df_marcas_fisico ------------------
CREATE TEMPORARY TABLE df_marcas_fisico AS (

    SELECT 
        codestrategia, 
        nrooferta,
        CONCAT_WS(" + ", COLLECT_LIST(desmarca)) AS marcaplan
    FROM (
        SELECT DISTINCT codestrategia, nrooferta, desmarca
        FROM df_tmp_atributos_ofertas_fis
        ) AS tmp
    GROUP BY codestrategia, nrooferta
    ORDER BY marcaplan ASC NULLS LAST

)

------------------ INICIO df_clases_fisico ------------------
CREATE TEMPORARY TABLE df_clases_fisico AS (

    SELECT 
        codestrategia, 
        nrooferta,
        CONCAT_WS(" + ", COLLECT_LIST(desclase)) AS claseplan
    FROM (
        SELECT DISTINCT codestrategia, nrooferta, desclase
        FROM df_tmp_atributos_ofertas_fis
        ) AS tmp
    GROUP BY codestrategia, nrooferta
    ORDER BY desclase ASC NULLS LAST

)

------------------ INICIO df_tmp_atributos_fisico_rec ------------------
CREATE TEMPORARY TABLE df_tmp_atributos_fisico_rec AS (

    SELECT 
        atr.codestrategia,
        atr.nrooferta,
        cla.claseplan,
        mar.marcaplan,
        atr.desmarca,
        atr.desclase,
        atr.unidades,
        atr.precionormalunitmn,
        atr.precioofertaunitmn
    FROM df_tmp_atributos_ofertas_fis atr
    LEFT JOIN df_clases_fisico cla 
    ON atr.codestrategia = cla.codestrategia
    AND atr.nrooferta = cla.nrooferta
        LEFT JOIN df_marcas_fisico mar 
        ON atr.codestrategia = mar.codestrategia
        AND atr.nrooferta = mar.nrooferta

)

------------------ INICIO df_tmp_atributos_fisico ------------------
CREATE TEMPORARY TABLE df_tmp_atributos_fisico AS (

    SELECT 
        codestrategia, 
        nrooferta, 
        claseplan, 
        marcaplan,
        CASE WHEN COUNT(DISTINCT desmarca) > 1 THEN 1 ELSE 0 END AS fmultimarca,
        CASE WHEN COUNT(DISTINCT desclase) > 1 THEN 1 ELSE 0 END AS fmulticlase,
        SUM(unidades) AS unidadesset,
        SUM(precionormalunitmn * unidades) AS precionormalsetmn,
        SUM(precioofertaunitmn * unidades) AS precioofertasetmn
    FROM df_tmp_atributos_fisico_rec
    GROUP BY codestrategia, nrooferta, claseplan, marcaplan

)

------------------ INICIO df_tmp_temporal_1 ------------------
CREATE TEMPORARY TABLE df_tmp_temporal_1 AS (

    SELECT 
        pr.codpais, 
        pr.aniocampana, 
        pr.codebelista, 
        pr.idoferta, 
        pr.orden, 
        pr.tactica, 
        pr.inputorigen, 
        pr.createddatesimulationreaction, 
        pr.oddcandidateid, 
        pr.mindiaodd, 
        pr.maxdiaodd, 
        pr.diasodd, 
        pr.procedencia, 
        pr.flgexposprior, 
        pr.cuc, 
        pr.cucpadre, 
        pr.precionormalunitmn, 
        pr.precioofertaunitmn, 
        pr.costounitmn, 
        pr.unidades, 
        pr.formatoplan, 
        pr.codventa, 
        pr.codsap, 
        pr.precionormalsetmn, 
        pr.precioofertasetmn, 
        pr.porcdescuentoset, 
        pr.costosetmn, 
        pr.flagmaterialganancia, 
        pr.flagcompuestavariable, 
        pr.factorcuadre, 
        pr.codventapadre, 
        pr.desmarca, 
        CASE 
            WHEN pr.desunidadnegocio = 'APOYO' THEN 'APOYO' 
            WHEN pr.desunidadnegocio = 'M&A' THEN 'M&A' 
            WHEN pr.desunidadnegocio = 'COSM' AND pr.desclase IN('ACCESOR.COSMETICOS') THEN 'ACC COSM' 
            WHEN pr.desunidadnegocio = 'COSM' AND pr.desclase IN('FRAGANCIAS') THEN 'FR' 
            WHEN pr.desunidadnegocio = 'COSM' AND pr.desclase IN('MAQUILLAJE') THEN 'MQ' 
            WHEN pr.desunidadnegocio = 'COSM' AND pr.desclase IN('TRATAMIENTO FACIAL') THEN 'TF' 
            WHEN pr.desunidadnegocio = 'COSM' AND pr.desclase IN('TRATAMIENTO CORPORAL') THEN 'TC' 
            WHEN pr.desunidadnegocio = 'COSM' AND pr.desclase IN('CUIDADO PERSONAL') THEN 'CP' 
            ELSE 'OTROS' 
        END AS desclase, 
        pr.desunidadnegocio, 
        pr.destipo, 
        pr.desproducto, 
        pr.descripcuc, 
        COALESCE(pr.fmulticlase, fis.fmulticlase) fmulticlase, 
        COALESCE(pr.claseoferta, fis.claseplan) claseoferta, 
        COALESCE(pr.fmultimarca, fis.fmultimarca) fmultimarca, 
        COALESCE(pr.marcaoferta, fis.marcaplan) marcaoferta, 
        pr.formatooferta, 
        pr.descripcionoferta, 
        COALESCE(pr.unidadesset, fis.unidadesset) unidadesset, 
        pr.desmarcaoferta, 
        pr.descategoriaoferta, 
        pr.comentario, 
        pr.fechaproceso, 
        pr.dispositivoajustado, 
        pr.desespacio, 
        pr.despopup, 
        pr.realvtamnneto, 
        pr.realvtadolneto, 
        pr.realuuvendidas, 
        pr.realcostomnvendidas, 
        pr.realcostodolvendidas, 
        pr.codestrategia, 
        pr.nrooferta, 
        pr.cantidadpedidospais, 
        pr.codtipooferta, 
        pr.realuufaltantes, 
        pr.realvtamnfaltneto, 
        pr.flagofertadigital, 
        pr.medioventa, 
        pr.flagresultados, 
        pr.flagreplicarevista, 
        pr.aniocampanaref,
        pr.flagofertafinal,
        pr.flagprodsugerido,
        pr.codcatalogo
    FROM  df_tmp_plan_res pr
    LEFT JOIN df_tmp_atributos_fisico fis
    ON pr.codestrategia = fis.codestrategia
    AND pr.nrooferta = fis.nrooferta

)

------------------ INICIO df_edp_asignacion_id_consultora_join ------------------
CREATE TEMPORARY TABLE df_edp_asignacion_id_consultora_join AS (

    SELECT DISTINCT
        codpais,
        aniocampana,
        idoferta,
        procedencia
    FROM df_edp_asignacion_id_consultora

)

------------------ INICIO df_cm_consultantsegment_x ------------------
CREATE TEMPORARY TABLE df_cm_consultantsegment_x AS (

    SELECT *
    FROM df_cm_consultantsegment
    WHERE codebelista = "XXXXXXXXX"

)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 2 - ETL_4 ----------------------------
----------------------- edptmp_consolidadoconsultora ----------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================

CREATE TEMPORARY TABLE edptmp_consolidadoconsultora AS (

    SELECT
        tmp.codpais,
        tmp.aniocampana,
        tmp.codebelista ,
        tmp.orden ,
        tmp.mindiaodd,
        tmp.maxdiaodd,
        tmp.diasodd,
        COALESCE(asi.procedencia, tmp.procedencia) AS procedencia,
        tmp.flgexposprior ,
        tmp.cuc ,
        COALESCE(tep.precionormalunitmn, tmp.precionormalunitmn) AS precionormalunitmn ,
        COALESCE(tep.precioofertaunitmn, tmp.precioofertaunitmn) AS precioofertaunitmn ,
        tep.unidades AS unidades ,
        tmp.codventa ,
        tmp.codsap,
        tmp.desmarca ,
        tmp.desclase ,
        tmp.desunidadnegocio ,
        tmp.destipo ,
        tmp.desproducto ,
        tmp.descripcuc ,
        COALESCE(cs.segmentorollingplan, cx.segmentorollingplan) AS segmentorollingplan ,
        COALESCE(cs.grupocompraplan, cx.grupocompraplan) AS grupocompraplan ,
        COALESCE(cs.desregionplan, cx.desregionplan) AS desregionplan ,
        COALESCE(cs.deszonaplan, cx.deszonaplan) AS deszonaplan ,
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
        tmp.flagofertadigital,
        tmp.medioventa,
        tmp.flagresultados,
        tmp.flagreplicarevista,
        tmp.aniocampanaref,
        tmp.flagofertafinal,
        tmp.flagprodsugerido,
        tmp.codcatalogo,
        COALESCE(tep.idoferta, tmp.idoferta) AS idoferta,
        COALESCE(tep.tactica, tmp.tactica) AS tactica,
        COALESCE(tep.inputorigen, tmp.inputorigen) AS inputorigen,
        tep.idofertaorigen,
        tep.reactioncandidateid,
        COALESCE(tep.createddatesimulationreaction, tmp.createddatesimulationreaction) AS createddatesimulationreaction,
        COALESCE(tep.oddcandidateid, tmp.oddcandidateid) AS oddcandidateid,
        COALESCE(tep.cucpadre, tmp.cucpadre) AS cucpadre,
        COALESCE(tep.costounitmn, tmp.costounitmn) AS costounitmn,
        COALESCE(tep.formatoplan, tmp.formatoplan) AS formatoplan,
        COALESCE(tep.precionormalsetmn, tmp.precionormalsetmn) AS precionormalsetmn,
        COALESCE(tep.precioofertasetmn, tmp.precioofertasetmn) AS precioofertasetmn,
        COALESCE(tep.porcdescuentoset, tmp.porcdescuentoset) AS porcdescuentoset,
        COALESCE(tep.costosetmn, tmp.costosetmn) AS costosetmn,
        COALESCE(tep.flagmaterialganancia, tmp.flagmaterialganancia) AS flagmaterialganancia,
        COALESCE(tep.flagcompuestavariable, tmp.flagcompuestavariable) AS flagcompuestavariable,
        tep.factorcuadre AS factorcuadre,
        COALESCE(tep.codventapadre, tmp.codventapadre) AS codventapadre,
        COALESCE(tep.fmulticlase, tmp.fmulticlase) AS fmulticlase,
        COALESCE(tep.claseplan, tmp.claseoferta) AS claseoferta,
        COALESCE(tep.fmultimarca, tmp.fmultimarca) AS fmultimarca,
        COALESCE(tep.marcaplan, tmp.marcaoferta) AS marcaoferta,
        COALESCE(tep.formatooferta, tmp.formatooferta) AS formatooferta,
        COALESCE(tep.descripcion, tmp.descripcionoferta) AS descripcionoferta,
        COALESCE(tep.unidadesset, tmp.unidadesset) AS unidadesset,
        COALESCE(tep.desmarcaoferta, tmp.desmarcaoferta) AS desmarcaoferta,
        COALESCE(tep.descategoriaoferta, tmp.descategoriaoferta) AS descategoriaoferta,
        COALESCE(tep.comentario, tmp.comentario) AS comentario
    FROM df_tmp_temporal_1 tmp
    LEFT JOIN df_edp_planeacion_oferta tep 
    ON tmp.codpais = tep.codpais
    AND tmp.aniocampana = tep.aniocampana
    AND tmp.codventa = tep.codventa
    AND tep.inputorigen != 'ARP'
        LEFT JOIN df_edp_asignacion_id_consultora_join asi 
        ON asi.codpais = tmp.codpais
        AND asi.aniocampana = tmp.aniocampana
        AND asi.idoferta = COALESCE(tep.idoferta, tmp.idoferta)
            LEFT JOIN df_cm_consultantsegment cs 
            ON tmp.codpais = cs.codpais
            AND tmp.aniocampana = cs.aniocampana
            AND TRIM(tmp.codebelista) = TRIM(cs.codebelista)
                LEFT JOIN df_cm_consultantsegment_x cx 
                ON tmp.codpais = cx.codpais
                AND tmp.aniocampana = cx.aniocampana;

)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 2 - ETL_4 ----------------------------
----------------------- edptmp_consolidadoconsultora ----------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================
