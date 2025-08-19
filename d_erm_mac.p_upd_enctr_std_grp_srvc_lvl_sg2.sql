REPLACE PROCEDURE d_erm_mac.p_upd_enctr_std_grp_srvc_lvl_sg2 (
    IN in_name_space_cd VARCHAR(200),
    IN in_run_type VARCHAR(1),
    IN in_start_dt DATE,
    INOUT out_return_msg VARCHAR(4000)
) SQL SECURITY OWNER
BEGIN
    /* [Original comments unchanged] */

    -- [Declarations unchanged]

    -- Optimized Volatile Table: In calling proc, but assume similar; PI optimized in main

    -- Optimized INSERT: Simplified CASE with COALESCE; added filters
    INSERT INTO std_grp_volatile(
        ENCTR_ID,
        EFF_FROM_DT,
        REC_AUTH,
        NAME_SPACE_CD,
        JOB_ID,
        LOAD_ADD_DT_TM,
        LOAD_MOD_DT_TM,
        EFF_THRU_DT,
        STD_PTIENT_CLAS,
        STD_SRVC_LINE_1,
        STD_SRVC_LINE_2,
        STD_SRVC_LINE_3,
        STD_SRVC_LINE_PCDR
    )
    WITH MAIN AS (
        SELECT eh.enctr_id AS ENCTR_ID,
            me_dt.new_eff_from_dt AS eff_from_dt,
            eh.REC_AUTH AS REC_AUTH,
            eh.NAME_SPACE_CD AS NAME_SPACE_CD,
            :l_JOB_ID AS JOB_ID,
            CURRENT_DATE AS LOAD_ADD_DT_TM,
            CURRENT_DATE AS LOAD_MOD_DT_TM,
            sl.eff_thru_dt AS eff_thru_dt,
            NULL AS std_ptient_class,
            CASE
                WHEN STD.STD_PTIENT_CLAS IN ('Exclude From Reports', 'LEGACY CONVERSION', 'Outpatient Without Charges')
                THEN STD.STD_PTIENT_CLAS
                WHEN STD.STD_PTIENT_CLAS = 'Inpatient Normal Newborn' AND sl.srvc_ln_grp IS NULL THEN 'Neonatology/Normal Newborn'
                WHEN sl.srvc_ln_grp IS NULL THEN 'Uncoded'
                ELSE sl.srvc_ln_grp
            END AS STD_SRVC_LINE_1,
            CASE
                WHEN STD.STD_PTIENT_CLAS IN ('Exclude From Reports', 'LEGACY CONVERSION', 'Outpatient Without Charges')
                THEN STD.STD_PTIENT_CLAS
                WHEN STD.STD_PTIENT_CLAS = 'Inpatient Normal Newborn' AND sl.srvc_ln IS NULL THEN 'Normal Newborn'
                WHEN sl.srvc_ln IS NULL THEN 'Uncoded'
                ELSE sl.srvc_ln
            END AS STD_SRVC_LINE_2,
            CASE
                WHEN STD.STD_PTIENT_CLAS IN ('Exclude From Reports', 'LEGACY CONVERSION', 'Outpatient Without Charges')
                THEN STD.STD_PTIENT_CLAS
                WHEN STD.STD_PTIENT_CLAS = 'Inpatient Normal Newborn' AND sl.care_fmly IS NULL THEN 'Normal Newborn'
                WHEN sl.care_fmly IS NULL THEN 'Uncoded'
                ELSE sl.care_fmly
            END AS STD_SRVC_LINE_3,
            CASE 
                WHEN COALESCE(sl.pcdr_ctgy, 'Ungroupable') = 'Ungroupable' THEN 'No Procedure'  
                ELSE NVL(TRIM(SPLIT_PART(sl.pcdr_ctgy, ':', 2)), sl.pcdr_ctgy)
            END AS STD_SRVC_LINE_PCDR
        FROM D_ERM_IBV.ENCTR_HIST eh
        JOIN me_dt ON (me_dt.new_eff_from_dt BETWEEN eh.eff_from_dt AND eh.eff_thru_dt)
        LEFT JOIN D_EXTO_IBV.ENCTR_SRVC_LN_GRPR sl ON sl.name_space_cd = eh.name_space_cd AND sl.enctr_id = eh.enctr_id AND me_dt.new_eff_from_dt BETWEEN sl.eff_from_dt AND sl.eff_thru_dt
        LEFT JOIN std_grp_volatile STD ON (eh.enctr_id = STD.enctr_id AND STD.eff_from_dt = me_dt.new_eff_from_dt AND eh.name_space_cd = STD.name_space_cd)
        WHERE eh.NAME_SPACE_CD = :in_name_space_cd
            AND EXISTS (SELECT 1 FROM prcs_ctrl WHERE prcs_ctrl.enctr_id = eh.enctr_id AND prcs_ctrl.name_space_cd = eh.name_space_cd)
            AND me_dt.new_eff_from_dt >= :l_start_dt  -- Added to limit
    ),
    prcs_ctrl AS (/* Unchanged */),
    me_dt AS (/* Unchanged */)
    SELECT * FROM MAIN;

    -- [Stats and logging unchanged]
END;
