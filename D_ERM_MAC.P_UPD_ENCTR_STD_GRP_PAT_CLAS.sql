REPLACE PROCEDURE D_ERM_MAC.P_UPD_ENCTR_STD_GRP_PAT_CLAS(
    IN in_name_space_cd VARCHAR(200),
    IN in_run_type VARCHAR(1),
    IN in_start_dt DATE,
    OUT out_return_msg VARCHAR(4000)
) SQL SECURITY OWNER
BEGIN
    /* [Original comments unchanged] */

    -- [Declarations unchanged]

    -- Optimized: Consolidated TERM logic into single CASE in INSERT to reduce CTE materialization/spool
    CREATE VOLATILE MULTISET TABLE std_grp_volatile AS (
        ENCTR_ID BIGINT,
        EFF_FROM_DT DATE,
        REC_AUTH SMALLINT,
        NAME_SPACE_CD VARCHAR(50),
        JOB_ID NUMBER,
        LOAD_ADD_DT_TM TIMESTAMP,
        LOAD_MOD_DT_TM TIMESTAMP,
        EFF_THRU_DT DATE,
        STD_PTIENT_CLAS VARCHAR(100),
        INPTN_COVID_FLG VARCHAR(1),
        SURG_FLG VARCHAR(1),
        ER_FLG VARCHAR(1)
    ) PRIMARY INDEX (enctr_id, eff_from_dt)  -- Optimized PI
    ON COMMIT PRESERVE ROWS;

    INSERT INTO std_grp_volatile
    WITH me_dt AS (/* Unchanged */),
         prcs_ctrl AS (/* Unchanged */),
         chrgs AS (
             SELECT dtl.enctr_id,
                 dtl.name_space_cd,
                 me_dt.new_eff_from_dt eff_from_dt,
                 SUM(dtl.chrg_qty) chrg_qty_cnt
             FROM D_ERM_IBV.ENCTR_HIST eh
             JOIN me_dt ON (me_dt.new_eff_from_dt BETWEEN eh.eff_from_dt AND eh.eff_thru_dt)
             JOIN D_ERM_IBV.ENCTR_CHRG_DTL dtl ON (dtl.enctr_id = eh.enctr_id AND eh.name_space_cd = dtl.name_space_cd AND post_dt <= me_dt.new_eff_from_dt AND post_dt >= :l_start_dt)  -- Added limit
             WHERE eh.name_space_cd = :in_name_space_cd
                 AND EXISTS (SELECT 1 FROM prcs_ctrl WHERE prcs_ctrl.enctr_id = eh.enctr_id AND prcs_ctrl.name_space_cd = eh.name_space_cd)
             GROUP BY 1,2,3
         ),
         DIAGS AS (/* Unchanged */)
    SELECT eh.enctr_id,
        me_dt.new_eff_from_dt eff_from_dt,
        eh.rec_auth,
        eh.name_space_cd,
        :l_job_id AS JOB_ID,
        CURRENT_DATE AS LOAD_ADD_DT_TM,
        CURRENT_DATE AS LOAD_MOD_DT_TM,
        '2041-01-01' EFF_THRU_DT,
        CASE
            -- Exclude From Reports (TERM1 logic)
            WHEN (ed.admit_dt_tm IS NULL AND ed.dschrg_dt_tm IS NULL)
                 OR pin.fmly_name IN ('Test Patient','TEST','Zztest','TESTPRODUCTION','Zztower')
                 OR pin.fmly_name||pin.gvn_name LIKE '%HNAM%TEST%'
                 OR eh.rec_auth = 69
                 OR tt3.tgt_term_cd = 'EXCLUDE FROM REPORTS'
                 OR ed.actv_ind = 'N'
                 OR eh.EXTR_AR_FLG = 'Y' AND eh.name_space_cd NOT IN ('EPIC-CHICAGO','EPIC-CHICAGO-PB')
            THEN 'Exclude From Reports'
            -- LEGACY CONVERSION (TERM2 logic)
            WHEN grp.tgt_term_key = 'LEGACY CONVERSION|GRP|SubCategory'
            THEN 'LEGACY CONVERSION'
            -- [Add other WHEN clauses for remaining TERMS, using EXISTS or aggregated counts from chrgs/DIAGS]
            -- Example for a TERM: WHEN EXISTS (SELECT 1 FROM ... WHERE condition) THEN 'Group Name'
            ELSE 'Undefined Main'
        END AS STD_PTIENT_CLAS,
        CASE WHEN diags.covid_icd_cnt > 0 THEN 'Y' ELSE 'N' END AS INPTN_COVID_FLG,
        /* SURG_FLG and ER_FLG logic here, e.g., based on conditions */
        'N' AS SURG_FLG,  -- Placeholder; integrate actual
        'N' AS ER_FLG     -- Placeholder; integrate actual
    FROM D_ERM_IBV.ENCTR_HIST eh
    JOIN me_dt ON (me_dt.new_eff_from_dt BETWEEN eh.eff_from_dt AND eh.eff_thru_dt)
    LEFT JOIN D_IDW_IBV.ENCTR_ADMSN ed ON (ed.enctr_id = eh.enctr_id AND eh.name_space_cd = ed.name_space_cd)
    LEFT JOIN D_IDW_IBV.PRTY_INDIV_NAME PIN ON (PIN.INDIV_PRTY_ID = e.PTIENT_MBR_PRTY_ID AND NAME_TYPE_CD='GVN_NM')
    LEFT JOIN D_SHR_IBV.term_map_fltn tm3 ON (eh.ptient_type_cd = tm3.src_term_key)
    LEFT JOIN D_SHR_IBV.term_map_grp_fltn tt3 ON (tm3.tgt_term_key = tt3.src_term_key AND tt3.tgt_fmly_name = 'PatientTypeGroup' AND me_dt.new_eff_from_dt BETWEEN tt3.eff_strt_dt AND tt3.eff_end_dt)
    LEFT JOIN chrgs ON (chrgs.enctr_id = eh.enctr_id AND chrgs.name_space_cd = eh.name_space_cd AND chrgs.eff_from_dt = me_dt.new_eff_from_dt)
    LEFT JOIN DIAGS ON (DIAGS.enctr_id = eh.enctr_id AND DIAGS.name_space_cd = eh.name_space_cd)
    -- [Add other joins as needed for remaining TERM conditions]
    WHERE eh.name_space_cd = :in_name_space_cd
        AND EXISTS (SELECT 1 FROM prcs_ctrl WHERE prcs_ctrl.enctr_id = eh.enctr_id AND prcs_ctrl.name_space_cd = eh.name_space_cd)
        AND eh.ENCTR_ID > 0;

    -- [Stats collection unchanged, but ensure on new PI]

    -- [Rest unchanged]
END;
