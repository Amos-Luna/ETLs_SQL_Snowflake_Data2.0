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
-- -----------------------------------------
-- =========================================

-- #####################################################################
--                              INPUT 1
--              df_cm_detailoffer_0 // edpcm_detailoffer
-- #####################################################################
CREATE TEMPORARY TABLE df_cm_detailoffer_0 AS (

    SELECT DISTINCT
        inputorigincode AS inputorigen,
        TRIM(flagtactic) AS tactica,
        formatcode AS formatooferta,
        comentario,
        TRIM(codventa) AS codventa,
        TRIM(cuc) AS cuc,
        TRIM(codsap) AS codsap,
        TRIM(cucpadre) AS cucpadre,
        CAST(precionormalmn AS DECIMAL(28,6)) AS precionormalsetmn,
        CAST(precioofertamn AS DECIMAL(28,6)) AS precioofertasetmn,
        CAST(discountpercentagetemination AS DECIMAL(28,6)) AS porcdescuentoset,
        CAST(setcost AS DECIMAL(28,6)) AS costosetmn,
        CAST(offerid AS BIGINT) AS idoferta,
        CAST(flaggainmaterial AS BOOLEAN) AS flagmaterialganancia,
        CAST(compoundvariable AS INT) AS flagcompuestavariable,
        CAST(quadfactor AS INT) AS factorcuadre,
        CAST(offeroriginid AS BIGINT) AS idofertaorigen,
        aniocampana,
        codpais,
        CAST(scenarioid AS BIGINT) AS scenarioid,
        TRIM(codventapadre) AS codventapadre,
        CAST(oddcandidateid AS BIGINT) AS oddcandidateid,
        CAST(reactioncandidateid AS BIGINT) AS reactioncandidateid,
        createddatesimulationreaction,
        marcaoferta AS desmarcaoferta,
        categoriaoferta AS descategoriaoferta,
        CAST(repetitionfactor AS INT) AS factorrepeticion
    FROM ENT_&{P_ENVIRONMENT}_CURATED_DB.CM.CM_DETAILOFFER
    WHERE codpais = $V_CODPAIS
        AND UUIDFILE = $P_UUIDFILE

)

-- #####################################################################
--                              INPUT 2
--               df_sap_dproducto // dwh_dproducto
-- #####################################################################
CREATE TEMPORARY TABLE df_sap_dproducto AS (

    SELECT
        desproducto, 
        codsap, 
        COALESCE(descripcuc, desproducto) as descripcuc, 
        desmarca, 
        descategoria, 
        desclase,
        desunidadnegocio as desnegocio, 
        destipo, 
        dessubcategoria, 
        destiposolo, 
        deslinea
    FROM dwh_dproducto

)

-- #####################################################################
--                              INPUT 3 
--               df_dm_dmatrizcampana // dwh_dmatrizcampana
-- #####################################################################
CREATE TEMPORARY TABLE df_dm_dmatrizcampana AS (

    SELECT DISTINCT 
        codpais,
        aniocampana, 
        codventa, 
        CAST(precionormalmn AS DECIMAL(30,6)) as precionormalunitmn,
        CAST(preciooferta AS DECIMAL(30,6)) as precioofertaunitmn
    FROM dwh_dmatrizcampana
    --GROUP BY codpais, aniocampana, codventa, precionormalmn, preciooferta
)

-- #####################################################################
--                              INPUT 4 
--               df_dc_dcostocamp // dwh_dcostoproductocampana
-- #####################################################################
CREATE TEMPORARY TABLE df_dc_dcostocamp AS (

    SELECT DISTINCT 
        codpais, 
        aniocampana, 
        codsap, 
        CAST(costoreposicionmn AS DECIMAL(30,6)) as costounitmn
    FROM dwh_dcostoproductocampana
    --GROUP BY codpais, aniocampana, codsap, costounitmn

)

------------------- INICIO df_cm_detailoffer ------------------------------
CREATE TEMPORARY TABLE df_cm_detailoffer AS (
    SELECT f_calculaaniocampana(df_cm_detailoffer_0, -4)
    FROM df_cm_detailoffer_0;
)

---------------------------------------------------------------------------------------
------------------ CONSTRUCCIÓN DE LA FUNCIÓN => f_calculaaniocampana ------------------
----------------------------------------------------------------------------------------

CREATE FUNCTION f_calculaaniocampana (df_deb TABLE, delta INT) RETURNS TABLE
BEGIN
    DECLARE df_db_0 TABLE;
    DECLARE df_db_1 TABLE;
    DECLARE df_db_2 TABLE;
    DECLARE result TABLE;
    
    INSERT INTO df_db_0
            SELECT 
                *,
                CASE 
                    WHEN TRIM(codpais) = 'PR' THEN 13 
                    WHEN TRIM(codpais) = 'EU' THEN 12 
                    WHEN TRIM(codpais) = 'BR' THEN 12 
                    ELSE 18 
                END AS nrocampanas,
                SUBSTRING(aniocampana, 1, 4) AS anio,
                CAST(SUBSTRING(aniocampana, 5, 6) AS INTEGER) AS campana
            FROM df_deb;
    
    INSERT INTO df_db_1 
            SELECT 
                *,
                CAST((campana + delta) AS INTEGER) AS sumacampanas
            FROM df_db_0;
    
    INSERT INTO df_db_2
            SELECT 
                *,
                CASE 
                    WHEN sumacampanas % nrocampanas != 0 THEN CAST((sumacampanas / nrocampanas) AS VARCHAR)
                    ELSE CAST(((sumacampanas / nrocampanas) - 1) AS VARCHAR)
                END AS extraanios,
                CASE 
                    WHEN sumacampanas != nrocampanas THEN CAST((sumacampanas % nrocampanas) AS VARCHAR)
                    ELSE CAST(nrocampanas AS VARCHAR)
                END AS extracampanias,
                (CAST(((-1 * sumacampanas / nrocampanas) + 1) AS INTEGER) * -1) AS menosanios,
                CASE 
                    WHEN sumacampanas % nrocampanas != 0 THEN CAST((sumacampanas % nrocampanas) AS VARCHAR)
                    ELSE CAST(nrocampanas AS VARCHAR)
                END AS menoscampanias
            FROM df_db_1;
    
    IF delta = 0 THEN
        INSERT INTO result 
                SELECT 
                    *,
                    aniocampana AS aniocampanaplan
                FROM df_deb;

    ELSE IF delta > 0 THEN
        INSERT INTO result 
                SELECT 
                    inputorigen,
                    tactica,
                    formatooferta,
                    comentario,
                    codventa,
                    cuc,
                    cucpadre,
                    codsap,
                    precionormalsetmn,
                    precioofertasetmn,
                    porcdescuentoset,
                    costosetmn,
                    idoferta,
                    flagmaterialganancia,
                    flagcompuestavariable,
                    factorcuadre,
                    idofertaorigen,
                    aniocampana,
                    codpais,
                    scenarioid,
                    codventapadre,
                    oddcandidateid,
                    reactioncandidateid,
                    createddatesimulationreaction,
                    desmarcaoferta,
                    descategoriaoferta,
                    factorrepeticion,
                    CASE 
                        WHEN extracampanias::INT < 10 THEN CONCAT(anio, extraanios, '0', extracampanias)
                        ELSE extracampanias 
                    END AS aniocampanaplan
                FROM df_db_2;

    ELSE
        INSERT INTO result 
                SELECT 
                    inputorigen, 
                    tactica, 
                    formatooferta, 
                    comentario, 
                    codventa, 
                    cuc, 
                    cucpadre, 
                    codsap, 
                    precionormalsetmn, 
                    precioofertasetmn, 
                    porcdescuentoset, 
                    costosetmn, 
                    idoferta, 
                    flagmaterialganancia, 
                    flagcompuestavariable, 
                    factorcuadre, 
                    idofertaorigen, 
                    aniocampana, 
                    codpais, 
                    scenarioid, 
                    codventapadre, 
                    oddcandidateid, 
                    reactioncandidateid, 
                    createddatesimulationreaction, 
                    desmarcaoferta, 
                    descategoriaoferta, 
                    factorrepeticion, 
                    CASE 
                        WHEN menoscampanias::INT < 10 THEN anio || CAST(menosanios AS VARCHAR) || '0' || menoscampanias 
                        ELSE menoscampanias 
                    END AS aniocampanaplan
                FROM df_db_2;
    END IF;
    RETURN result;
END;
----------------------------------------------------------------------------------------------
------------------ FIN - CONSTRUCCIÓN DE LA FUNCIÓN => f_calculaaniocampana ------------------
----------------------------------------------------------------------------------------------

-- ######## =====>>>  2da FORMA - CONSTRUCCIÓN DE LA FUNCIÓN A UTILIZAR ///////////////

-- CREATE FUNCTION calcula_aniocampana(delta INT, codpais VARCHAR, aniocampana VARCHAR) RETURNS VARCHAR
-- BEGIN
--     DECLARE nrocampanas INT;
--     DECLARE anio INT;
--     DECLARE campana INT;
--     DECLARE sumacampanas INT;
--     DECLARE extraanios INT;
--     DECLARE extracampanias INT;
--     DECLARE menosanios INT;
--     DECLARE menoscampanias INT;
--     DECLARE aniocampanaplan VARCHAR;
    
--     SET nrocampanas = CASE TRIM(codpais)
--         WHEN 'PR' THEN 13
--         WHEN 'EU' THEN 12
--         WHEN 'BR' THEN 12
--         ELSE 18
--         END;
    
--     SET anio = SUBSTR(aniocampana, 1, 4);
--     SET campana = CAST(SUBSTR(aniocampana, 5, 6) AS INT);
    
--     SET sumacampanas = campana + delta;
    
--     IF delta = 0 THEN
--         SET aniocampanaplan = aniocampana;
--     ELSE IF delta > 0 THEN
--         SET extraanios = (CASE WHEN sumacampanas % nrocampanas != 0 THEN CAST(sumacampanas / nrocampanas AS INT) ELSE CAST((sumacampanas / nrocampanas) - 1 AS INT) END);
--         SET extracampanias = (CASE WHEN sumacampanas != nrocampanas THEN CAST(sumacampanas % nrocampanas AS VARCHAR) ELSE CAST(nrocampanas AS VARCHAR) END);
--         SET aniocampanaplan = CONCAT(anio + extraanios, (CASE WHEN CAST(extracampanias AS INT) < 10 THEN CONCAT('0', extracampanias) ELSE extracampanias END));
--     ELSE
--         SET menosanios = (((sumacampanas * -1) / nrocampanas) + 1) * -1;
--         SET menoscampanias = (CASE WHEN sumacampanas % nrocampanas != 0 THEN CAST(sumacampanas % nrocampanas AS VARCHAR) ELSE CAST(nrocampanas AS VARCHAR) END);
--         SET aniocampanaplan = CONCAT(anio + menosanios, (CASE WHEN CAST(menoscampanias AS INT) < 10 THEN CONCAT('0', menoscampanias) ELSE menoscampanias END));
--     END IF;
    
--     RETURN aniocampanaplan;
-- END;


-- SELECT 
--     inputorigen, 
--     tactica, 
--     formatooferta, 
--     comentario, 
--     codventa, 
--     cuc, 
--     cucpadre, 
--     codsap, 
--     precionormalsetmn, 
--     precioofertasetmn, 
--     porcdescuentoset, 
--     costosetmn, 
--     idoferta, 
--     flagmaterialganancia, 
--     flagcompuestavariable, 
--     factorcuadre, 
--     idofertaorigen, 
--     aniocampana, 
--     codpais, 
--     scenarioid, 
--     codventapadre, 
--     oddcandidateid, 
--     reactioncandidateid, 
--     createddatesimulationreaction, 
--     desmarcaoferta, 
--     descategoriaoferta, 
--     factorrepeticion, 
--     calcula_aniocampana(-4, codpais, aniocampana) AS aniocampanaplan 
-- FROM df_cm_detailoffer
---- //////////////////////////////////////////////////////////////////////////////////////

-------------------- INICIO df_do_units ------------------------------
CREATE TEMPORARY TABLE df_do_units AS (
    SELECT *,
        CASE
            WHEN factorcuadre >= 1 THEN factorcuadre
            ELSE 1 * factorrepeticion
        END AS unidades
    FROM df_cm_detailoffer;
)

-------------------- INICIO df_do_units2 ------------------------------
CREATE TEMPORARY TABLE df_do_units2 AS (
    SELECT DISTINCT 
        codpais, 
        aniocampana, 
        cucpadre, 
        idoferta, 
        unidades
    FROM df_do_units;
)

-------------------- INICIO df_planeacion_ofertas_0 ------------------------------
CREATE TEMPORARY TABLE df_planeacion_ofertas_0 AS (
    SELECT 
        cd.codpais AS codpais, 
        cd.aniocampana AS aniocampana, 
        cd.aniocampanaplan AS aniocampanaplan, 
        cd.idoferta AS idoferta, 
        cd.cuc AS cuc, 
        cd.cucpadre AS cucpadre, 
        dm.precionormalunitmn AS precionormalunitmn, 
        dm.precioofertaunitmn AS precioofertaunitmn, 
        dc.costounitmn AS costounitmn, 
        du.unidades AS unidades, 
        cd.reactioncandidateid AS reactioncandidateid, 
        cd.createddatesimulationreaction AS createddatesimulationreaction, 
        cd.oddcandidateid AS oddcandidateid, 
        cd.idofertaorigen AS idofertaorigen, 
        cd.inputorigen AS inputorigen, 
        cd.tactica AS tactica, 
        cd.formatooferta AS formatooferta, 
        cd.comentario AS comentario, 
        cd.codventa AS codventa, 
        cd.codsap AS codsap, 
        cd.precionormalsetmn AS precionormalsetmn, 
        cd.precioofertasetmn AS precioofertasetmn, 
        cd.porcdescuentoset AS porcdescuentoset, 
        cd.costosetmn AS costosetmn, 
        cd.flagmaterialganancia AS flagmaterialganancia, 
        cd.flagcompuestavariable AS flagcompuestavariable, 
        cd.factorcuadre AS factorcuadre, 
        cd.codventapadre AS codventapadre, 
        dd.desmarca AS desmarca, 
        dd.descategoria AS descategoria, 
        dd.desclase AS desclase, 
        dd.desnegocio AS desnegocio, 
        dd.destipo AS destipo, 
        dd.dessubcategoria AS dessubcategoria, 
        dd.destiposolo AS destiposolo, 
        dd.deslinea AS deslinea, 
        dd.desproducto AS desproducto, 
        dd.descripcuc AS descripcuc, 
        cd.desmarcaoferta AS desmarcaoferta, 
        cd.descategoriaoferta AS descategoriaoferta
    FROM df_cm_detailoffer AS cd
        INNER JOIN df_dm_dmatrizcampana AS dm
            ON cd.codpais = dm.codpais 
            AND cd.aniocampana = dm.aniocampana
            AND cd.codventapadre = dm.codventa
                LEFT JOIN df_dc_dcostocamp AS dc
                    ON cd.codpais = dc.codpais
                    AND cd.aniocampana = dc.aniocampana
                    AND cd.codsap = dc.codsap
                        LEFT JOIN df_sap_dproducto AS dd
                            ON cd.codsap = dd.codsap
                                LEFT JOIN df_do_units2 AS du
                                    ON cd.codpais = du.codpais
                                    AND cd.aniocampana = du.aniocampana
                                    AND cd.cucpadre = du.cucpadre
                                    AND cd.idoferta = du.idoferta
    ORDER BY 
        cd.codpais ASC, 
        cd.aniocampana ASC, 
        cd.aniocampanaplan ASC NULLS LAST, 
        cd.idoferta ASC NULLS LAST
)

-------------------- INICIO df_planeacion_ofertas_multi_marca ---------------------------
CREATE TEMPORARY TABLE df_planeacion_ofertas_multi_marca AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta,
        CASE
            WHEN COUNT(DISTINCT desmarca) > 1 THEN 1 
            ELSE 0 
        END AS fmultimarca
    FROM df_planeacion_ofertas_0
    GROUP BY codpais, aniocampana, aniocampanaplan, idoferta
)


-------------------- INICIO df_marcas_ofertas ---------------------------
CREATE TEMPORARY TABLE df_marcas_ofertas AS (

    SELECT
        a.*, 
        b.*
    FROM df_planeacion_ofertas_0 a
    INNER JOIN df_planeacion_ofertas_multi_marca b
        ON a.codpais = b.codpais
        AND a.aniocampana = b.aniocampana
        AND a.aniocampanaplan = b.aniocampanaplan
        AND a.idoferta = b.idoferta;

)


-------------------- INICIO df_ofertas_multimarca_a ---------------------------
CREATE TEMPORARY TABLE df_ofertas_multimarca_a AS (

    SELECT
        codpais,
        aniocampana,
        aniocampanaplan,
        idoferta,
        fmultimarca,
        desmarca AS desmarca_0
    FROM df_marcas_ofertas
)


-------------------- INICIO df_ofertas_multimarca_b ---------------------------
CREATE TEMPORARY TABLE df_ofertas_multimarca_b AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        CONCAT_WS(' + ', COLLECT_LIST(desmarca_0)) AS desmarca --- ???? LINEA 260 SPARK
    FROM df_ofertas_multimarca_a
    GROUP BY codpais, aniocampana, aniocampanaplan, idoferta
    ORDER BY desmarca ASC NULLS LAST;

)

-------------------- INICIO df_ofertas_multimarca ---------------------------
CREATE TEMPORARY TABLE df_ofertas_multimarca AS (

    SELECT DISTINCT 
        a.codpais AS codpais, 
        a.aniocampana AS aniocampana, 
        a.aniocampanaplan AS aniocampanaplan, 
        a.idoferta AS idoferta, 
        a.fmultimarca AS fmultimarca, 
        b.desmarca AS desmarca
    FROM df_ofertas_multimarca_a a
    INNER JOIN df_ofertas_multimarca_b b
        ON a.codpais = b.codpais 
        AND a.aniocampana = b.aniocampana 
        AND a.aniocampanaplan = b.aniocampanaplan 
        AND a.idoferta = b.idoferta
)

-------------------- INICIO df_planeacion_ofertas_multi_clase ---------------------------
CREATE TEMPORARY TABLE df_planeacion_ofertas_multi_clase AS (

    SELECT
        codpais,
        aniocampana,
        aniocampanaplan,
        idoferta,
        CASE 
            WHEN COUNT(DISTINCT desclase) > 1 THEN 1 
            ELSE 0 
        END AS fmulticlase
    FROM df_planeacion_ofertas_0
    GROUP BY codpais, aniocampana, aniocampanaplan, idoferta

)

-------------------- INICIO df_clase_ofertas_0 ---------------------------
CREATE TEMPORARY TABLE df_clase_ofertas_0 AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta,
        CASE 
            WHEN TRIM(desclase) = 'ACCESORIOS COSMETICOS' THEN 'ACC COSM'
            WHEN TRIM(desclase) = 'CUIDADO PERSONAL' THEN 'CP'
            WHEN TRIM(desclase) = 'FRAGANCIAS' THEN 'FR'
            WHEN TRIM(desclase) = 'MAQUILLAJE' THEN 'MQ'
            WHEN TRIM(desclase) = 'TRATAMIENTO CORPORAL' THEN 'TC'
            WHEN TRIM(desclase) = 'TRATAMIENTO FACIAL' THEN 'TF'
            WHEN TRIM(desclase) IN ('BIJOUTERIE', 'COMPLEMENTOS', 'HOGAR', 'LENTES', 'RELOJES') THEN 'M&A'
            WHEN TRIM(desclase) IN ('INCENTIVOS', 'MATERIAL APOYO A VENTA', 'MATERIAL APOYO MKT', 'PROMOCION USUARIO') THEN 'TERCEROS'
            ELSE 'OTROS'
        END AS claseplan_0
    FROM df_planeacion_ofertas_0 a
    INNER JOIN df_planeacion_ofertas_multi_clase b
        ON a.codpais = b.codpais
        AND a.aniocampana = b.aniocampana
        AND a.aniocampanaplan = b.aniocampanaplan
        AND a.idoferta = b.idoferta
)

-------------------- INICIO df_clase_ofertas_1 ---------------------------
CREATE TEMPORARY TABLE df_clase_ofertas_1 AS (

    SELECT DISTINCT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        claseplan_0, 
        fmulticlase
    FROM df_clase_ofertas_0
)

-------------------- INICIO df_clase_ofertas_1 ---------------------------
CREATE TEMPORARY TABLE df_ofertas_multiclase_a AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        CONCAT_WS(" + ", COLLECT_LIST(claseplan_0)) AS claseplan 
    FROM df_clase_ofertas_1
    GROUP BY codpais, aniocampana, aniocampanaplan, idoferta
    ORDER BY claseplan_0 ASC NULLS LAST;

)

-------------------- INICIO df_ofertas_multiclase ---------------------------
CREATE TEMPORARY TABLE df_ofertas_multiclase AS (

    SELECT DISTINCT 
        a.codpais AS codpais, 
        a.aniocampana AS aniocampana, 
        a.aniocampanaplan AS aniocampanaplan, 
        a.idoferta AS idoferta, 
        a.fmulticlase AS fmulticlase, 
        b.claseplan AS claseplan
    FROM df_clase_ofertas_1 a
    INNER JOIN df_ofertas_multiclase_a b 
            ON a.codpais = b.codpais 
            AND a.aniocampana = b.aniocampana 
            AND a.aniocampanaplan = b.aniocampanaplan 
            AND a.idoferta = b.idoferta;


)

-------------------- INICIO df_formato_oferta_0 ---------------------------
CREATE TEMPORARY TABLE df_formato_oferta_0 AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        flagcompuestavariable, 
        formatooferta, 
        COUNT(DISTINCT cuc) AS cuc, 
        COUNT(DISTINCT cucpadre) AS cucpadre
    FROM df_planeacion_ofertas_0
    GROUP BY codpais, aniocampana, aniocampanaplan, idoferta, flagcompuestavariable, formatooferta;

)

-------------------- INICIO df_formato_oferta_1 ---------------------------
CREATE TEMPORARY TABLE df_formato_oferta_1 AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        flagcompuestavariable, 
        formatooferta, 
        CASE 
            WHEN TRIM(formatooferta) = 'B' AND TRIM(flagcompuestavariable) = '1' AND TRIM(cucpadre) = '1' THEN 'I' 
            ELSE TRIM(formatooferta) 
        END AS formatoplan
    FROM df_formato_oferta_0;

)

-------------------- INICIO df_descripcion_oferta_pre ---------------------------
CREATE TEMPORARY TABLE df_descripcion_oferta_pre AS (

    SELECT *, 
        CASE 
            WHEN descripcuc IS NULL OR TRIM(descripcuc) = '' THEN desproducto 
            ELSE descripcuc 
        END AS producto 
    FROM df_planeacion_ofertas_0;

)

-------------------- INICIO df_descripcion_oferta_0 ---------------------------
CREATE TEMPORARY TABLE df_descripcion_oferta_0 AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        cucpadre, 
        formatooferta, 
        MIN(unidades) AS unidades, 
        MIN(factorcuadre) AS factorcuadre, 
        MIN(producto) AS producto 
    FROM df_descripcion_oferta_pre
    GROUP BY codpais, aniocampana, aniocampanaplan, idoferta, cucpadre, formatooferta;

)

-------------------- INICIO df_descripcion_oferta_1 ---------------------------
CREATE TEMPORARY TABLE df_descripcion_oferta_1 AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        formatooferta, 
        cucpadre, 
        producto, 
        unidades, 
        factorcuadre,
        CASE 
            WHEN unidades >= factorcuadre THEN unidades 
            ELSE factorcuadre 
        END AS unidadesaux,
        CASE 
            WHEN unidades >= factorcuadre THEN unidades 
            ELSE factorcuadre 
        END AS descripcion_0
    FROM df_descripcion_oferta_0

)

-------------------- INICIO df_descripcion_oferta_2 ---------------------------
CREATE TEMPORARY TABLE df_descripcion_oferta_2 AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        formatooferta, 
        cucpadre, 
        producto, 
        unidades, 
        factorcuadre, 
        unidadesaux, 
        CONCAT(CAST(descripcion_0 AS varchar(255)), 'X ', COALESCE(producto, '')) AS descripcion
    FROM df_descripcion_oferta_1

)

-------------------- INICIO df_descripcion_oferta_3_a ---------------------------
CREATE TEMPORARY TABLE df_descripcion_oferta_3_a AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        CONCAT_WS(' + ', COLLECT_LIST(descripcion) ORDER BY descripcion) AS descripcion_end
    FROM df_descripcion_oferta_2
    GROUP BY codpais, aniocampana, aniocampanaplan, idoferta
    ORDER BY descripcion_end ASC NULLS LAST

)

-------------------- INICIO df_unidadesset_tmp ---------------------------
CREATE TEMPORARY TABLE df_unidadesset_tmp AS (

    SELECT 
        codpais, 
        aniocampana, 
        idoferta, 
        SUM(unidadesaux) AS unidadesaux
    FROM df_descripcion_oferta_2
    GROUP BY codpais, aniocampana, idoferta

)

-------------------- INICIO df_descripcion_oferta_3 ---------------------------
CREATE TEMPORARY TABLE df_descripcion_oferta_3 AS (

    SELECT 
        a.codpais AS codpais, 
        a.aniocampana AS aniocampana, 
        a.aniocampanaplan AS aniocampanaplan, 
        a.idoferta AS idoferta, 
        c.unidadesaux AS unidadesset, 
        b.descripcion AS descripcion
    FROM df_descripcion_oferta_2 a
        INNER JOIN df_descripcion_oferta_3_a b
        ON a.codpais = b.codpais 
        AND a.aniocampana = b.aniocampana 
        AND a.aniocampanaplan = b.aniocampanaplan 
        AND a.idoferta = b.idoferta
        INNER JOIN df_unidadesset_tmp c
            ON a.codpais = c.codpais 
            AND a.aniocampana = c.aniocampana 
            AND a.idoferta = c.idoferta

)

-------------------- INICIO df_descripcion_oferta_join ---------------------------
CREATE TEMPORARY TABLE df_descripcion_oferta_join AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        MIN(descripcion) as descripcion, 
        MIN(unidadesset) as unidadesset
    FROM df_descripcion_oferta_3
    GROUP BY codpais, aniocampana, aniocampanaplan, idoferta

)

-------------------- INICIO df_ofertas_multimarca_join ---------------------------
CREATE TEMPORARY TABLE df_ofertas_multimarca_join AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        fmultimarca, 
        desmarca as marcaplan
    FROM df_ofertas_multimarca

)

-------------------- INICIO df_formato_oferta_join ---------------------------
CREATE TEMPORARY TABLE df_formato_oferta_join AS (

    SELECT 
        codpais, 
        aniocampana, 
        aniocampanaplan, 
        idoferta, 
        formatoplan 
    FROM df_formato_oferta_1

)

-------------------- INICIO df_planeacionofertacm ---------------------------
CREATE TEMPORARY TABLE df_planeacionofertacm AS (

    SELECT
        plan.codpais AS codpais,
        plan.aniocampana AS aniocampana,
        plan.aniocampanaplan AS aniocampanaplan,
        plan.idoferta AS idoferta,
        plan.cuc AS cuc,
        plan.cucpadre AS cucpadre,
        plan.precionormalunitmn AS precionormalunitmn,
        plan.precioofertaunitmn AS precioofertaunitmn,
        plan.costounitmn AS costounitmn,
        plan.unidades AS unidades,
        plan.reactioncandidateid AS reactioncandidateid,
        plan.createddatesimulationreaction AS createddatesimulationreaction,
        plan.oddcandidateid AS oddcandidateid,
        plan.idofertaorigen AS idofertaorigen,
        plan.inputorigen AS inputorigen,
        plan.tactica AS tactica,
        plan.formatooferta AS formatooferta,
        plan.comentario AS comentario,
        plan.codventa AS codventa,
        plan.codsap AS codsap,
        plan.precionormalsetmn AS precionormalsetmn,
        plan.precioofertasetmn AS precioofertasetmn,
        plan.porcdescuentoset AS porcdescuentoset,
        plan.costosetmn AS costosetmn,
        plan.flagmaterialganancia AS flagmaterialganancia,
        plan.flagcompuestavariable AS flagcompuestavariable,
        plan.factorcuadre AS factorcuadre,
        plan.codventapadre AS codventapadre,
        plan.desmarca AS desmarca,
        plan.descategoria AS descategoria,
        plan.desclase AS desclase,
        plan.desnegocio AS desnegocio,
        plan.destipo AS destipo,
        plan.dessubcategoria AS dessubcategoria,
        plan.destiposolo AS destiposolo,
        plan.deslinea AS deslinea,
        plan.desproducto AS desproducto,
        plan.descripcuc AS descripcuc,
        mul.fmulticlase AS fmulticlase,
        mul.claseplan AS claseplan,
        jo.fmultimarca AS fmultimarca,
        jo.marcaplan AS marcaplan,
        for.formatoplan AS formatoplan,
        ofer.descripcion AS descripcion,
        ofer.unidadesset AS unidadesset,
        plan.desmarcaoferta AS desmarcaoferta,
        plan.descategoriaoferta AS descategoriaoferta
    FROM df_planeacion_ofertas_0 plan
    INNER JOIN df_ofertas_multiclase mul 
        ON plan.codpais = mul.codpais AND
        plan.aniocampana = mul.aniocampana AND
        plan.aniocampanaplan = mul.aniocampanaplan AND
        plan.idoferta = mul.idoferta
            INNER JOIN df_ofertas_multimarca_join jo 
            ON plan.codpais = jo.codpais AND
            plan.aniocampana = jo.aniocampana AND
            plan.aniocampanaplan = jo.aniocampanaplan AND
            plan.idoferta = jo.idoferta
                INNER JOIN df_formato_oferta_join for 
                ON plan.codpais = for.codpais AND
                plan.aniocampana = for.aniocampana AND
                plan.aniocampanaplan = for.aniocampanaplan AND
                plan.idoferta = for.idoferta
                INNER JOIN df_descripcion_oferta_join ofer 
                ON plan.codpais = ofer.codpais AND
                plan.aniocampana = ofer.aniocampana AND
                plan.aniocampanaplan = ofer.aniocampanaplan AND
                plan.idoferta = ofer.idoferta

)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL2 ----------------------------
------------------- edpda_planeacionoferta -------------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================

CREATE TEMPORARY TABLE edpda_planeacionoferta AS (

    CREATE TEMPORARY TABLE df_planeacionofertacm_auxtable AS (
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
            tactica, 
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
            descategoria, 
            desclase, 
            desnegocio, 
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
            ROW_NUMBER() OVER (PARTITION BY 
                                    codpais, 
                                    aniocampana, 
                                    idoferta, 
                                    codventa 
                                ORDER BY inputorigen DESC NULLS LAST,
                                        flagmaterialganancia DESC NULLS LAST, 
                                        idofertaorigen ASC NULLS LAST) AS fila
        FROM df_planeacionofertacm
        WHERE idoferta IS NOT NULL 
            AND codventa IS NOT NULL
    )
    SELECT *,
        '&{P_IDCARGA}'  AS IDCARGA, ---- ??????
        '&{P_UUID_PROCESS}' AS UUIDFILE, ---- ??????
        fecproceso = CURRENT_TIMESTAMP() ---- ??????
    FROM df_planeacionofertacm_auxtable
    WHERE fila = 1;

)

-- =================!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!==================
--------------------------- OUTPUT 1 - ETL2 ----------------------------
------------------- edpda_planeacionoferta -------------------------
-- =================¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡=================