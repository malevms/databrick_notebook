REPLACE PROCEDURE D_ERM_MAC.P_UPD_ENCTR_STD_GRP_HIST (
    IN in_name_space_cd	VARCHAR(200),
    IN in_run_type		VARCHAR(1),
    IN in_start_dt		DATE,
    OUT out_return_msg VARCHAR(4000)
    ) SQL SECURITY OWNER
BEGIN

     /* p_upd_enctr_std_grp_hist
      * Desc: 		This proc will call all Patient Classification and all three service line proc
      * 				Complete Delete and Load (ERM is rebuildable from ODS)
      * 				All Date-Effectivity will be Month-End and Current Date-1
      *
      * Parameters:	in_name_space_cd: The name_space_cd associated with this job
      *				in_run_type: 'I' for incremental (Daily) or 'H' for a full historic load
      *
      * Usage:		call D_ERM_MAC.p_upd_enctr_std_grp_hist('HEALTHQUEST OH1CONS', 'H', null, xx);
      * 				call D_ERM_MAC.p_upd_enctr_std_grp_hist('HEALTHQUEST OH1CONS', 'I', null, xx);
      * 				call D_ERM_MAC.p_upd_enctr_std_grp_hist('HEALTHQUEST OH1CONS', 'S', to_date('2018-01-31','yyyy-mm-dd'), xx);
      * Version:		0.3.5
      *
      *
      * TODO:
      * 		- change the left join condtion. move the condition from where to ON() clause.
      * 		- Admit and Discharge date should be pulled from ENCTR_HIST table.
      *          - Get rid of SQLSTATE '42000' dangerous.
      *
      *Change Log:
      *  Date 			Who				What
      * 12/16/2016		Kumar, Alok		Init
      * 09/16/2017      pat             Fixed l_JOB_ID warning.
      * 03/12/2018      pat             changed ENCTR_STD_GRP_HIST delete to 2 days to match "pat_clas , srvc_lvl3, srvc_lvl2 and srvc_lvl1 "
      * 05/08/2018		Kumar, Alok		Added the logic for the run type as 'S'.
      * 05/18/2018      pat             Modified Exception handling, modified logging only 1 rec instead of 1 for START and 1 for END. changed "in_last_completion_dt to in_start_dt &
      * 								l_last_completion_dt to l_start_dt" and changed "out_return_msg" to OUT only.
      * 06/07/2018		Kumar, Alok		Updated the :l_start_dt-1 from DELETE so it will delete only 1 day prior to current date.
      * 07/02/2018		Kumar, Alok		Added a new merge SQL to load the encounters with any change in ENCTR_HIST, AGG and CHRG_DTL.
      * 								Also, the encounters with UEM Patient Class as null.
      * 07/09/2018	   	Kumar, Alok		Removed -1 from DELETE where :l_start_dt - 1 because we do not want to re-process yesterday's data.
      * 								Also added logic to re-load the encounters in the process control table for any data issue in the PAM VW or change in the ERM Tables.
      * 09/10/2018		Kumar, Alok		Added volatile table logic and swiched the INSERT to MERGE while loading the ENCTR_STD_GRP_HIST table to avoid UNIQUE constraint issue.
      * 10/08/2018		Kumar, Alok		Added new Merge SQL that will insert/update into Process cdontrol table for the encounters which needs re-processing by Standard Grouping.
      * 10/16/2018		Kumar, Alok		Replaced Hard coded 'EPIC-CHICAGO' with :in_name_space_cd from the merge sql.
      * 11/07/2018		Kumar, Alok		Removed rec_auth from the merge sql for Process Control table.
      * 11/13/2018		Kumar, Alok		Updated the Merge SQL for process control to include missing encounters from Standard Grouping table.
      * 11/14/2018		Kumar, Alok		Added REC_AUTH back in the MERGE SQL for Process control table.
      * 11/20/2018		Kumar, Alok		Updated the Merge SQL for process control to exclude the encounters which already exists.
      * 11/26/2018		Kumar, Alok		Updated the merge SQL to exclude the negative encounters completely.
      * 11/27/2018		Kumar, Alok		Created seperate insert our of Merge SQL for any missing encounter in the Standard Grouping table.
      * 11/27/2018		Kumar, Alok		Updated the insert SQL for the process control load.
      * 12/20/2018		Kumar, Alok		Updated the Merge SQL for the process control load to replace STG table with INT table.
      * 01/15/2019		Kumar, Alok		Added condition for high date for ENCTR_HIST table in the MERGE sql for Process Control load.
      * 02/14/2019		Kumar, Alok		Removed the Merge SQL that updates the data from STG Process control to INT table. No longer needed.
      *									Fixed the incremental load by updateing the Script name while pulling the last successful run from Job history.
      * 06/07/3019      PAT             Re-pointed the *_RSW sp's to the regular ones.
      * 07/08/2019      PAT     		As part of ENCTR's (RSW) that have multiple ENCTR_ID's based on REC_AUTH's change, pick the most recent ENCTR_ID.
      * 11/21/2019      PAT             Re-placed the script_name from p_upd_enctr_std_grp_hist_rsw to p_upd_enctr_std_grp_hist so that the INCREMENTAL code wont DELETE all the data.
      * 01/20/2020		vd				Added process to force service lvl 2 and 1 to be same as lvl 3 when lvl 3 is Legacy Conversion
      * 02/24/2020		vd				Commented collect stat for p_erm_stg.stg_enctr_std_grp_hist and p_erm_int.enctr_std_grp_hist
      * 04/15/2020      PAT             Removed error propogation hurdle from pat_clas, srvc_lvl1/2/3 to STD_GRP_HIST. As part of this, need to have
      * 								exclusive DROP scripts for "std_grp_volatile & std_grp_hist_volatile" while running from Teradata client.
      * 06/30/2020      PAT             clean up un-wanted code.
      * 08/01/2020		VD				Commented service line 1 call.
      * 12/01/2020      PAT             Updated "H" start date to "2020-01-01" (for H/I/S, delete happens based on date.OLD "H" refresh loads >=2019-04-01).
      * 								as long as any older data is maintained, we should be good. currently no new RSW sites and new EPIC_TC will have no hist data.
      * 12/16/2020		VD				Changed the procedure call for level 2(p_upd_enctr_std_grp_srvc_lvl_2) to new proc name for both lvl1 and lvl2
      * 05/14/2021      PAT             updated ld_mod_dt_tm part of last UPDATE for enctr_prcs_cntrl. REPLACED *_ACCV to *IBV.
      * 06/09/2021		VD				Added new field INPTN_COVID_FLG to the grouping/volatile table
      * 04/06/2022		VD				Modified to include SURG_FLG & ER_FLG from patient classification
      * 11/17/2022		VD				Updated to include the flags in the compare logic to generate unique record for std_grp_hist_volatile
      * 								also updated the history date to 2022-01-01
  	  * 05/19/2023		DM				commented out call to p_upd_enctr_std_grp_srvc_lvl_1_2 and p_upd_enctr_std_grp_srvc_lvl3 and added call to p_upd_enctr_std_grp_srvc_lvl_sg2
  	  * 05/25/2023		AGD				Moved INPTN_COVID_FLG logic from srvc_lvl_3 to pat_clas for insert into the stage table.
	  * 06/12/2023		DM				moving Sl1 , sl2 update logic to p_upd_enctr_std_grp_srvc_lvl_sg2. increases size of std_srvc_lvl_1,2,3 from 100 to 200.
	  * 11/08/2023		Anne			Added new SG2 procedure category as STD_SRVC_LINE_PCDR. Renamed STD_SRVC_LVL_# to STD_SRVC_LINE_# and LD_%_DT_TM to LOAD_%_DT_TM.
	  *                                 Removed 'NO FALLBACK' volatile table setting since is not valid for TD cloud.
	  * 04/30/2025		VD				Added EFF_FROM_DT,REC_AUTH to the volatile table primary index
  	  *
      */

        DECLARE l_last_completion_dt, l_start_dt DATE;
        DECLARE not_found, l_activity_count NUMBER  DEFAULT 0; --l_dur_min
        DECLARE l_warn_msg 					VARCHAR(255); --l_invoker
        DECLARE l_PRCS_NAME                	VARCHAR(55) DEFAULT 'Standard Grouping Load RSW';
        DECLARE l_SCRIPT_NAME              	VARCHAR(55) DEFAULT 'p_upd_enctr_std_grp_hist';
        DECLARE l_STAT_CD                  	VARCHAR(55) DEFAULT 'R';
        DECLARE l_BUS_UNIT					VARCHAR(55) DEFAULT 'ALL';
        DECLARE l_JOB_ID,l_JOB_DTL_ID		NUMBER DEFAULT NULL; -- will get a new one is null
        DECLARE l_RUN_ERROR_CD 				NUMBER DEFAULT 0;
        DECLARE l_RUN_ERROR_MSG,l_return_msg VARCHAR(2000);

        DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET not_found = 1;
        DECLARE CONTINUE HANDLER FOR SQLWARNING SET l_warn_msg = 'WARN '||SQLCODE;
                --	DECLARE CONTINUE HANDLER FOR SQLSTATE '42000' BEGIN END;  -- drop table
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            SET l_RUN_ERROR_CD = SQLCODE;
            GET DIAGNOSTICS EXCEPTION 1 l_RUN_ERROR_MSG = MESSAGE_TEXT;
            CALL D_ERM_MAC.P_ERM_JOB_HISTORY(:l_JOB_ID, :l_PRCS_NAME, :l_BUS_UNIT, :l_SCRIPT_NAME, 'E', 0, 0, 0, :l_RUN_ERROR_CD, :l_SCRIPT_NAME||' failed with error '||:l_RUN_ERROR_MSG,  :l_JOB_ID);
            RESIGNAL SET MESSAGE_TEXT  =  l_RUN_ERROR_MSG; -- will see only 128 characters in TDE (limitation).

        END;
                	-- get default start times
        SET l_BUS_UNIT = in_name_space_cd;
        SET out_return_msg =  'Started';
        SELECT COALESCE(MAX(start_dt), DATE'2013-01-01') INTO l_last_completion_dt
            FROM D_SHR_IBV.JOB_HISTORY
            WHERE script_name = 'p_upd_enctr_std_grp_hist'
                AND  stat_cd = 'C'
                AND  business_unit = :in_name_space_cd;

        IF in_run_type = 'H' THEN
            SET l_start_dt = DATE'2022-07-01';
        ELSEIF in_run_type = 'I' THEN
            SET l_start_dt = l_last_completion_dt;
        ELSEIF in_run_type = 'S' THEN
            SET l_start_dt = in_start_dt;
        END IF;

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY(:l_JOB_ID, :l_PRCS_NAME, :l_BUS_UNIT, :l_SCRIPT_NAME, :l_STAT_CD, 0, 0, 0,
        :l_RUN_ERROR_CD, :l_RUN_ERROR_MSG,  :l_JOB_ID);

                  ---- WE NEED TO REVISIT THIS CODE BASE ON THE OTHER CHANGE IN THE QA.
                	---- job detail start STG_ENCTR_TSACTN_DTL
        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
        'Start of Insert/Update records in the Stage Process Control STG table - The Param Values are: in_run_type: '||:in_run_type
        ||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'
        ||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

                MERGE INTO D_ERM_IBV.enctr_prcs_cntrl cntrl
                USING (
                SELECT eh.enctr_id,
                    eh.name_space_cd,
                    eh.rec_auth
                    FROM D_ERM_IBV.ENCTR_HIST eh
                    JOIN D_ERM_IBV.ENCTR_CHRG_DTL DTL ON eh.enctr_id = dtl.enctr_id AND eh.name_space_cd = dtl.name_space_cd
                    WHERE eh.NAME_SPACE_CD  = :in_name_space_cd --				and cast(eh.enctr_sk as bigint) not in( select enctr_id*-1 FROM D_ERM_IBV.ENCTR_PRCS_CNTRL ctrl where name_space_cd = :in_name_space_cd ) --not needed.

                        AND  eh.EFF_THRU_DT = '2041-01-01'
                        AND  COALESCE(dtl.chrg_qty,0) > 0
                        AND  dtl.post_dt >= CURRENT_DATE -31
                    GROUP BY 1,
                        2,
                        3
                ) enctrs ON (cntrl.enctr_id = enctrs.enctr_id AND cntrl.name_space_cd = enctrs.name_space_cd)
                    WHEN MATCHED THEN UPDATE
                SET ld_mod_dt_tm 	  =  CURRENT_TIMESTAMP(6),
                    std_grpg_rsult_dt =  NULL,
                    job_id 			  = :l_JOB_ID;

                SET l_activity_count = activity_count;

                CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, l_activity_count,
                'END of Insert/Update records in the Stage Process Control STG table - The Param Values are: in_run_type: '||:in_run_type
                ||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'
                ||
                CASE
                    WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
                END, l_JOB_DTL_ID);


                --    CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
                --    'Start of Insert/Update records in the Process Control STG table - The Param Values are: in_run_type: '||:in_run_type
                --    		||' ;name_space_cd:'||:in_name_space_cd||' ;in_start_dt'
                --    		||case when :in_start_dt is null then ' NO DATE ' else cast((:in_start_dt (format 'yyyy-mm-dd')) as varchar(10) ) end, l_JOB_DTL_ID);
                --
                --	   merge into D_ERM_IBV.enctr_prcs_cntrl cntrl
                --        using(select * from D_ERM_SBV.stg_enctr_prcs_cntrl stg
                --		where stg.name_space_cd = :in_name_space_cd ) enctrs
                --       on (cntrl.enctr_id = enctrs.enctr_id and cntrl.name_space_cd = enctrs.name_space_cd)
                --       when matched then update
                --              set ld_mod_dt_tm =  enctrs.ld_mod_dt_tm,
                --                  std_grpg_rsult_dt   = enctrs.std_grpg_rsult_dt,
                --                  job_id = enctrs.job_id
                --       when not matched then insert
                --              values (enctrs.enctr_id, enctrs.rec_auth, enctrs.name_space_cd, enctrs.job_id, enctrs.ld_add_dt_tm, enctrs.ld_mod_dt_tm,
                --                           enctrs.drg_grouper_extc_dt, enctrs.drg_grouper_rsult_dt, enctrs.std_grpg_extc_dt, enctrs.std_grpg_rsult_dt);
                --
                --		SET l_activity_count = activity_count;
                --
                --    CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, l_activity_count,
                --    'END of Insert/Update records in the Process Control STG table - The Param Values are: in_run_type: '||:in_run_type
                --    		||' ;name_space_cd:'||:in_name_space_cd||' ;in_start_dt'
                --    		||case when :in_start_dt is null then ' NO DATE ' else cast((:in_start_dt (format 'yyyy-mm-dd')) as varchar(10) ) end, l_JOB_DTL_ID);

                CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
                'Start of Insert records in the Process Control table  - The Param Values are: in_run_type: '||:in_run_type
                ||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'
                ||
                CASE
                    WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
                END, l_JOB_DTL_ID);

                		--Seperate insert for any missing encounter in the Standard Grouping table.
                INSERT INTO D_ERM_IBV.ENCTR_PRCS_CNTRL
                SELECT eh.enctr_id,
                    eh.rec_auth,
                    eh.name_space_cd,
                    :l_JOB_ID,
                    CURRENT_TIMESTAMP(6),
                    CURRENT_TIMESTAMP(6),
                    CURRENT_DATE,
                    NULL,
                    CURRENT_DATE,
                    NULL,
					CURRENT_DATE, -- Added by Hari for Sg2
                    NULL -- Added by Hari for Sg2

                    FROM D_ERM_IBV.enctr_hist eh
                    LEFT JOIN D_ERM_IBV.enctr_std_grp_hist std ON eh.enctr_id = std.enctr_id AND eh.name_space_cd = std.name_space_cd
                    WHERE eh.name_space_cd = :in_name_space_cd
                        AND  eh.eff_thru_dt = '2041-01-01'
                        AND  std.enctr_id IS NULL
                        AND  eh.enctr_id > 0
                        AND  eh.enctr_id NOT IN(
                    SELECT e.enctr_id
                        FROM (
                        SELECT *
                            FROM D_IDW_IBV.ENCTR
                            QUALIFY ROW_NUMBER() OVER ( PARTITION BY src_admn_enctr_sk, src_admn_name_space_cd
                            ORDER BY load_mod_dt_tm DESC,
                                load_add_dt_tm DESC )=1
                        ) e
                        JOIN D_ERM_IBV.ENCTR_PRCS_CNTRL ctrl  ON  ( e.src_admn_name_space_cd = ctrl.name_space_cd AND ( e.enctr_id = ctrl.enctr_id OR CAST(SRC_ADMN_ENCTR_SK AS BIGINT)= CAST(ctrl.enctr_id AS BIGINT)*-1 ) )
                        WHERE src_admn_name_space_cd = :in_name_space_cd)
                    GROUP BY 1,
                        2,
                        3; -- leave it here for now, might be some bad data back then.

                CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, l_activity_count,
                'END of Insert records in the Process Control table - The Param Values are: in_run_type: '||:in_run_type
                ||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'
                ||
                CASE
                    WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
                END, l_JOB_DTL_ID);

                	---- job detail start STG_ENCTR_TSACTN_DTL
                CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
                'Start of DROP/CREATE std_grp_volatile table - The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
                CASE
                    WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
                END, l_JOB_DTL_ID);

                --	drop table std_grp_volatile;
        CREATE VOLATILE TABLE std_grp_volatile, /*NO FALLBACK,*/ NO JOURNAL, NO LOG
        (
            ENCTR_ID BIGINT,
            EFF_FROM_DT DATE,
            REC_AUTH SMALLINT,
            NAME_SPACE_CD VARCHAR(50),
            JOB_ID DECIMAL(38,0),
            LOAD_ADD_DT_TM TIMESTAMP(6),
            LOAD_MOD_DT_TM TIMESTAMP(6),
            EFF_THRU_DT DATE,
            STD_PTIENT_CLAS VARCHAR(100),
            STD_SRVC_LINE_1 VARCHAR(200),
            STD_SRVC_LINE_2 VARCHAR(200),
            STD_SRVC_LINE_3 VARCHAR(200),
            STD_SRVC_LINE_PCDR VARCHAR(200),
            INPTN_COVID_FLG VARCHAR(1),
            SURG_FLG VARCHAR(1),
            ER_FLG VARCHAR(1))
        PRIMARY INDEX (enctr_id,EFF_FROM_DT,REC_AUTH) ON COMMIT PRESERVE ROWS;

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
        'END of DROP/CREATE std_grp_volatile table - The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

                    /* Calling Standard Grouping Patient Classification proc --------- */
        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
        'Start calling p_upd_enctr_std_grp_pat_clas  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

        CALL D_ERM_MAC.p_upd_enctr_std_grp_pat_clas (:in_name_space_cd, :in_run_type,l_start_dt,l_return_msg);

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
        'End calling p_upd_enctr_std_grp_pat_clas  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);


                	/*No collecting stats for Volatile table.*/
        COLLECT STATISTICS COLUMN (NAME_SPACE_CD) 						ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID ,EFF_FROM_DT) 				ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID) 							ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID ,EFF_FROM_DT,NAME_SPACE_CD) ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID ,EFF_FROM_DT,REC_AUTH)		ON std_grp_volatile;

                    /* Calling Standard Grouping Service Line-3 proc   ----------------*/
        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
        'Start calling p_upd_enctr_std_grp_srvc_lvl_sg2  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

--        CALL D_ERM_MAC.p_upd_enctr_std_grp_srvc_lvl3 (:in_name_space_cd, :in_run_type, l_start_dt, l_return_msg); -- Disabled for Sg2
        CALL D_ERM_MAC.p_upd_enctr_std_grp_srvc_lvl_sg2 (:in_name_space_cd, :in_run_type, l_start_dt, l_return_msg);

		CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
        'End calling p_upd_enctr_std_grp_srvc_lvl_sg2  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

                	/*No collecting stats for Volatile table.*/
        COLLECT STATISTICS COLUMN (NAME_SPACE_CD) 						ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID ,EFF_FROM_DT) 				ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID) 							ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID ,EFF_FROM_DT,NAME_SPACE_CD) ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID ,EFF_FROM_DT, REC_AUTH)		ON std_grp_volatile;

                    /* Calling Standard Grouping Service Line-2 proc ---------------- */
       /* CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
        'Start calling p_upd_enctr_std_grp_srvc_lvl2  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

        CALL D_ERM_MAC.p_upd_enctr_std_grp_srvc_lvl_1_2 (:in_name_space_cd, :in_run_type , l_start_dt, l_return_msg);

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
        'End calling  p_upd_enctr_std_grp_srvc_lvl2 The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);
		*/

                	/*No collecting stats for Volatile table.*/
        /*
		COLLECT STATISTICS COLUMN (NAME_SPACE_CD) 						ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID ,EFF_FROM_DT) 				ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID) 							ON std_grp_volatile;
        COLLECT STATISTICS COLUMN (ENCTR_ID ,EFF_FROM_DT,NAME_SPACE_CD) ON std_grp_volatile;

       */
	   /* Calling Standard Grouping Service Line-1 proc ----------------
                    CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
                    'Start calling p_upd_enctr_std_grp_srvc_lvl1  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;in_start_dt'||case when :in_start_dt is null then ' NO DATE ' else cast((:in_start_dt (format 'yyyy-mm-dd')) as varchar(10) ) end, l_JOB_DTL_ID);

                	--	call D_ERM_MAC.p_upd_enctr_std_grp_srvc_lvl1 (:in_name_space_cd, :in_run_type, l_start_dt, l_return_msg);

                    CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
                    'End calling p_upd_enctr_std_grp_srvc_lvl1  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;in_start_dt'||case when :in_start_dt is null then ' NO DATE ' else cast((:in_start_dt (format 'yyyy-mm-dd')) as varchar(10) ) end, l_JOB_DTL_ID);
                    */
        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
        'Start of Insert into  STG_ENCTR_STD_GRP_HIST  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

        DELETE
        FROM D_ERM_SBV.STG_ENCTR_STD_GRP_HIST
        WHERE name_space_cd = in_name_space_cd
            AND  (:in_run_type IN ('H','I','S') );

        INSERT INTO D_ERM_SBV.STG_ENCTR_STD_GRP_HIST(
        ENCTR_ID,
        EFF_FROM_DT,
        REC_AUTH,
        NAME_SPACE_CD,
        JOB_ID,
        LOAD_ADD_DT_TM,
        LOAD_MOD_DT_TM,
        EFF_THRU_DT,
        STD_PTIENT_CLAS,
        STD_SRVC_LINE_3,
        STD_SRVC_LINE_2,
        STD_SRVC_LINE_1,
        STD_SRVC_LINE_PCDR,
        INPTN_COVID_FLG,
        SURG_FLG,
        ER_FLG
        )
        WITH
        main AS(
        SELECT pat_clas.enctr_id,
            pat_clas.eff_from_dt,
            pat_clas.rec_auth,
            pat_clas.name_space_cd,
            pat_clas.eff_thru_dt,
            pat_clas.std_ptient_clas,
            srvc_line_3.STD_SRVC_LINE_3,
            srvc_line_2.STD_SRVC_LINE_2,
            srvc_line_1.STD_SRVC_LINE_1,
            srvc_line_p.STD_SRVC_LINE_PCDR,
            pat_clas.INPTN_COVID_FLG,
            pat_clas.SURG_FLG,
            pat_clas.ER_FLG
            FROM pat_clas
            LEFT JOIN srvc_line_3 ON(pat_clas.enctr_id = srvc_line_3.enctr_id AND pat_clas.eff_from_dt = srvc_line_3.eff_from_dt AND pat_clas.name_space_cd = srvc_line_3.name_space_cd)
            LEFT JOIN srvc_line_2 ON(pat_clas.enctr_id = srvc_line_2.enctr_id AND pat_clas.eff_from_dt = srvc_line_2.eff_from_dt AND pat_clas.name_space_cd = srvc_line_2.name_space_cd)
            LEFT JOIN srvc_line_1 ON(pat_clas.enctr_id = srvc_line_1.enctr_id AND pat_clas.eff_from_dt = srvc_line_1.eff_from_dt AND pat_clas.name_space_cd = srvc_line_1.name_space_cd)
            LEFT JOIN srvc_line_p ON(pat_clas.enctr_id = srvc_line_p.enctr_id AND pat_clas.eff_from_dt = srvc_line_p.eff_from_dt AND pat_clas.name_space_cd = srvc_line_p.name_space_cd)
            GROUP BY pat_clas.enctr_id,
                pat_clas.eff_from_dt,
                pat_clas.rec_auth,
                pat_clas.name_space_cd,
                pat_clas.eff_thru_dt,
                pat_clas.std_ptient_clas,
                srvc_line_3.STD_SRVC_LINE_3,
                srvc_line_2.STD_SRVC_LINE_2,
                srvc_line_1.STD_SRVC_LINE_1,
                srvc_line_p.STD_SRVC_LINE_PCDR,
                pat_clas.INPTN_COVID_FLG ,
            	pat_clas.SURG_FLG,
            	pat_clas.ER_FLG
        ),
        pat_clas AS
        (
        SELECT enctr_id,
            eff_from_dt,
            rec_auth,
            name_space_cd,
            eff_thru_dt,
            std_ptient_clas,
            INPTN_COVID_FLG,
            SURG_FLG,
            ER_FLG
            FROM std_grp_volatile
            WHERE enctr_id >0
                AND  std_ptient_clas IS NOT NULL
            GROUP BY enctr_id,
                eff_from_dt,
                rec_auth,
                name_space_cd,
                eff_thru_dt,
                std_ptient_clas,
                INPTN_COVID_FLG,
            	SURG_FLG,
            	ER_FLG),
        srvc_line_3 AS
        (
        SELECT enctr_id,
            eff_from_dt,
            rec_auth,
            name_space_cd,
            eff_thru_dt,
            STD_SRVC_LINE_3
            --INPTN_COVID_FLG
            FROM std_grp_volatile
            WHERE enctr_id >0
                AND  STD_SRVC_LINE_3 IS NOT NULL
            GROUP BY enctr_id,
                eff_from_dt,
                rec_auth,
                name_space_cd,
                eff_thru_dt,
                STD_SRVC_LINE_3),
                --INPTN_COVID_FLG),
        srvc_line_2 AS
        (
        SELECT enctr_id,
            eff_from_dt,
            rec_auth,
            name_space_cd,
            eff_thru_dt,
            STD_SRVC_LINE_2
            FROM std_grp_volatile
            WHERE enctr_id >0
                AND  STD_SRVC_LINE_2 IS NOT NULL
            GROUP BY enctr_id,
                eff_from_dt,
                rec_auth,
                name_space_cd,
                eff_thru_dt,
                STD_SRVC_LINE_2),
        srvc_line_1 AS
        (
        SELECT enctr_id,
            eff_from_dt,
            rec_auth,
            name_space_cd,
            eff_thru_dt,
            STD_SRVC_LINE_1
            FROM std_grp_volatile
            WHERE enctr_id >0
                AND  STD_SRVC_LINE_1 IS NOT NULL
            GROUP BY enctr_id,
                eff_from_dt,
                rec_auth,
                name_space_cd,
                eff_thru_dt,
                STD_SRVC_LINE_1),
		srvc_line_p AS
        (
        SELECT enctr_id,
            eff_from_dt,
            rec_auth,
            name_space_cd,
            eff_thru_dt,
            STD_SRVC_LINE_PCDR
            FROM std_grp_volatile
            WHERE enctr_id >0
                AND  STD_SRVC_LINE_PCDR IS NOT NULL
            GROUP BY enctr_id,
                eff_from_dt,
                rec_auth,
                name_space_cd,
                eff_thru_dt,
                STD_SRVC_LINE_PCDR)
        SELECT enctr_id,
            eff_from_dt,
            rec_auth,
            name_space_cd,
            :l_JOB_ID,
            CURRENT_DATE AS LOAD_ADD_DT_TM,
            CURRENT_DATE AS LOAD_MOD_DT_TM,
            eff_thru_dt,
            std_ptient_clas,
            STD_SRVC_LINE_3,
            STD_SRVC_LINE_2,
            STD_SRVC_LINE_1,
            STD_SRVC_LINE_PCDR,
            INPTN_COVID_FLG,
            SURG_FLG,
            ER_FLG
            FROM main
            GROUP BY enctr_id,
                eff_from_dt,
                rec_auth,
                name_space_cd,
                :l_JOB_ID,
                CURRENT_DATE, -- as LOAD_ADD_DT_TM,
                CURRENT_DATE, -- as LOAD_MOD_DT_TM,
                eff_thru_dt,
                std_ptient_clas,
                STD_SRVC_LINE_3,
                STD_SRVC_LINE_2,
                STD_SRVC_LINE_1,
                STD_SRVC_LINE_PCDR,
                INPTN_COVID_FLG,
            	SURG_FLG,
            	ER_FLG;
  /*  Moving logic to p_upd_enctr_std_grp_srvc_lvl_sg2 on 6/12/2023
        UPDATE D_ERM_SBV.STG_ENCTR_STD_GRP_HIST
        SET std_srvc_lvl_2 = std_srvc_lvl_3, std_srvc_lvl_1 = std_srvc_lvl_3
        WHERE std_srvc_lvl_3 IN('Exclude From Reports', 'Outpatients Without Charges', 'LEGACY CONVERSION','Inpatient Normal Newborn') -- 01/20/2020 Added Legacy Conversion

            AND  name_space_cd = :in_name_space_cd;
*/

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
        'End of Insert into  STG_ENCTR_STD_GRP_HIST  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);


        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
        'Start of Merge into  ENCTR_STD_GRP_HIST  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

        DELETE
        FROM D_ERM_IBV.ENCTR_STD_GRP_HIST
        WHERE NAME_SPACE_CD = :in_NAME_SPACE_CD
            AND  eff_from_dt > :l_start_dt
            AND  (:in_run_type IN ('H','I','S') );

                	-- drop table std_grp_hist_volatile;
        CREATE VOLATILE TABLE std_grp_hist_volatile, /*NO FALLBACK,*/ NO JOURNAL, NO LOG
        (
            ENCTR_ID BIGINT,
            EFF_FROM_DT DATE,
            REC_AUTH SMALLINT,
            NAME_SPACE_CD VARCHAR(50),
            JOB_ID DECIMAL(38,0),
            EFF_THRU_DT DATE,
            STD_PTIENT_CLAS VARCHAR(100),
            STD_SRVC_LINE_1 VARCHAR(200),
            STD_SRVC_LINE_2 VARCHAR(200),
            STD_SRVC_LINE_3 VARCHAR(200),
            STD_SRVC_LINE_PCDR VARCHAR(200),
            INPTN_COVID_FLG VARCHAR(1),
            SURG_FLG VARCHAR(1),
            ER_FLG VARCHAR(1))
        PRIMARY INDEX (enctr_id) ON COMMIT PRESERVE ROWS;

        INSERT INTO std_grp_hist_volatile --D_ERM_IBV.ENCTR_STD_GRP_HIST
        (ENCTR_ID,
        EFF_FROM_DT,
        REC_AUTH,
        NAME_SPACE_CD,
        JOB_ID,
        EFF_THRU_DT,
        STD_PTIENT_CLAS,
        std_srvc_line_1,
        std_srvc_line_2,
        std_srvc_line_3,
        std_srvc_line_pcdr,
        INPTN_COVID_FLG,
        SURG_FLG,
        ER_FLG )
        WITH stagable AS(
        SELECT ROW_NUMBER() OVER (PARTITION BY enctr_id
            ORDER BY eff_from_dt) row_num,
                x.*
            FROM (
            SELECT stg1.enctr_id,
                stg1.EFF_FROM_DT,
                stg1.EFF_THRU_DT,
                stg1.REC_AUTH,
                stg1.NAME_SPACE_CD,
                stg1.STD_PTIENT_CLAS,
                stg1.std_srvc_line_3,
                stg1.std_srvc_line_2,
                stg1.std_srvc_line_1,
                stg1.std_srvc_line_pcdr,
                stg1.INPTN_COVID_FLG,
            	stg1.SURG_FLG,
            	stg1.ER_FLG,
                MAX(COALESCE(stg1.STD_PTIENT_CLAS,'NA')||COALESCE(stg1.std_srvc_line_3,'NA')|| COALESCE(stg1.std_srvc_line_2,'NA')||COALESCE(stg1.std_srvc_line_1,'NA')
                ||COALESCE(stg1.std_srvc_line_pcdr,'NA')||COALESCE(stg1.INPTN_COVID_FLG,'NA')||COALESCE(stg1.SURG_FLG,'NA')||COALESCE(stg1.ER_FLG,'NA'))
                OVER(PARTITION BY stg1.enctr_id, stg1.REC_AUTH, stg1.NAME_SPACE_CD
                ORDER BY eff_From_dt ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) rm
                FROM D_ERM_SBV.STG_ENCTR_STD_GRP_HIST stg1
                WHERE stg1.NAME_SPACE_CD = :in_NAME_SPACE_CD
            ) x
            WHERE COALESCE( rm,'N/A') <> (COALESCE(STD_PTIENT_CLAS,'NA')||COALESCE(std_srvc_line_3,'NA')|| COALESCE(std_srvc_line_2,'NA')||COALESCE(std_srvc_line_1,'NA')
                                        ||COALESCE(std_srvc_line_pcdr,'NA')||COALESCE(INPTN_COVID_FLG,'NA')||COALESCE(SURG_FLG,'NA')||COALESCE(ER_FLG,'NA'))
        )
        SELECT stagable.ENCTR_ID,
            stagable.EFF_FROM_DT,
            stagable.REC_AUTH,
            stagable.NAME_SPACE_CD,
            :l_JOB_ID,
            stagable.EFF_THRU_DT,
            stagable.STD_PTIENT_CLAS,
            stagable.std_srvc_line_1,
            stagable.std_srvc_line_2,
            stagable.std_srvc_line_3,
            stagable.std_srvc_line_pcdr,
            stagable.INPTN_COVID_FLG,
            stagable.SURG_FLG,
            stagable.ER_FLG
            FROM stagable
            LEFT JOIN D_ERM_IBV.ENCTR_STD_GRP_HIST std 	ON(stagable.row_num = 1 AND stagable.name_space_cd = std.name_space_cd
            AND stagable.enctr_id = std.enctr_id AND std.eff_thru_dt = '2041-01-01')
            WHERE stagable.NAME_SPACE_CD = :in_NAME_SPACE_CD
                AND  (COALESCE(std.STD_PTIENT_CLAS,'NA')||COALESCE(std.std_srvc_line_3,'NA')|| COALESCE(std.std_srvc_line_2,'NA')||COALESCE(std.std_srvc_line_1,'NA')
                    ||COALESCE(std.std_srvc_line_pcdr,'NA')||COALESCE(std.INPTN_COVID_FLG,'NA')||COALESCE(std.SURG_FLG,'NA')||COALESCE(std.ER_FLG,'NA'))
            <> (COALESCE(stagable.STD_PTIENT_CLAS,'NA')||COALESCE(stagable.std_srvc_line_3,'NA')|| COALESCE(stagable.std_srvc_line_2,'NA')||COALESCE(stagable.std_srvc_line_1,'NA')
              ||COALESCE(stagable.std_srvc_line_pcdr,'NA')||COALESCE(stagable.INPTN_COVID_FLG,'NA')||COALESCE(stagable.SURG_FLG,'NA')||COALESCE(stagable.ER_FLG,'NA'))
                OR  stagable.row_num <> 1;

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
        'End of Insert into std_grp_hist_volatile; The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
        'Start of Merge into  ENCTR_STD_GRP_HIST  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);


        MERGE INTO D_ERM_IBV.ENCTR_STD_GRP_HIST x USING(
        SELECT ENCTR_ID,
            EFF_FROM_DT,
            REC_AUTH,
            NAME_SPACE_CD,
            JOB_ID,
            EFF_THRU_DT,
            STD_PTIENT_CLAS,
            STD_SRVC_LINE_1,
            STD_SRVC_LINE_2,
            STD_SRVC_LINE_3,
            STD_SRVC_LINE_PCDR,
            INPTN_COVID_FLG,
            ER_FLG,
            SURG_FLG
            FROM std_grp_hist_volatile
        )y ON (x.enctr_id = y.enctr_id AND x.NAME_SPACE_CD = y.NAME_SPACE_CD AND x.eff_from_dt = y.eff_from_dt)
            WHEN MATCHED THEN UPDATE SET
        JOB_ID 			= :l_JOB_ID,
        LOAD_MOD_DT_TM 		= CURRENT_TIMESTAMP(6),
        STD_PTIENT_CLAS 	= Y.STD_PTIENT_CLAS,
        STD_SRVC_LINE_1 	= Y.STD_SRVC_LINE_1,
        STD_SRVC_LINE_2 	= Y.STD_SRVC_LINE_2,
        STD_SRVC_LINE_3 	= Y.STD_SRVC_LINE_3,
        STD_SRVC_LINE_PCDR  = Y.STD_SRVC_LINE_PCDR,
        INPTN_COVID_FLG 	= Y.INPTN_COVID_FLG,
        SURG_FLG = Y.SURG_FLG,
        ER_FLG = Y.ER_FLG
            WHEN NOT MATCHED THEN
        INSERT VALUES(
        Y.ENCTR_ID,
        Y.EFF_FROM_DT,
        Y.REC_AUTH,
        Y.NAME_SPACE_CD,
        :l_JOB_ID,
        CURRENT_TIMESTAMP(6),
        CURRENT_TIMESTAMP(6),
        Y.EFF_THRU_DT,
        Y.STD_PTIENT_CLAS,
        Y.STD_SRVC_LINE_1,
        Y.STD_SRVC_LINE_2,
        Y.STD_SRVC_LINE_3,
        Y.STD_SRVC_LINE_PCDR,
        Y.INPTN_COVID_FLG,
        Y.ER_FLG,
        Y.SURG_FLG) ;

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
        'End of Insert into ENCTR_STD_GRP_HIST;  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
        'Start updating ENCTR_STD_GRP_HIST  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

        UPDATE xx
        FROM D_ERM_IBV.ENCTR_STD_GRP_HIST xx,
            (
        SELECT name_space_cd,
            enctr_id,
            eff_from_dt,
            COALESCE((MAX(eff_from_dt) OVER (PARTITION BY name_space_cd,enctr_id
            ORDER BY eff_from_dt ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)-1),CAST('20410101' AS DATE FORMAT 'yyyymmdd')) next_dt
            FROM D_ERM_IBV.ENCTR_STD_GRP_HIST) x SET eff_thru_dt = x.next_dt
        WHERE x.name_space_cd = xx.name_space_cd
            AND  x.eff_from_dt = xx.eff_from_dt
            AND  x.enctr_id = xx.enctr_id
            AND  xx.name_space_cd = in_name_space_cd;

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
        'End updating ENCTR_STD_GRP_HIST  The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

        SET l_activity_count = activity_count;

                	/*
                	 * Now updating Process control table.
                	 */
        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, NULL, 0,
        'Start Update ENCTR_PRCS_CNTRL The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

        UPDATE D_ERM_IBV.enctr_prcs_cntrl ctrl
        SET std_grpg_rsult_dt = CURRENT_DATE, ld_mod_dt_tm = CURRENT_TIMESTAMP(6) , job_id  = :l_JOB_ID
        WHERE ctrl.name_space_cd = :in_name_space_cd
            AND  ctrl.enctr_id IN (
        SELECT std.enctr_id
            FROM D_ERM_IBV.ENCTR_STD_GRP_HIST std
            WHERE std.NAME_SPACE_CD = :in_NAME_SPACE_CD
            GROUP BY 1 )
            AND  (ctrl.std_grpg_rsult_dt IS NULL OR ctrl.std_grpg_rsult_dt < ctrl.std_grpg_extc_dt);

        CALL D_ERM_MAC.P_ERM_JOB_HISTORY_DTL(:l_JOB_ID, :l_JOB_DTL_ID, 0,
        'End Update ENCTR_PRCS_CNTRL The Param Values are: in_run_type: '||:in_run_type||' ;name_space_cd:'||:in_name_space_cd||' ;l_start_dt'||
        CASE
            WHEN :l_start_dt IS NULL THEN ' NO DATE ' ELSE CAST((:l_start_dt (FORMAT 'yyyy-mm-dd')) AS VARCHAR(10) )
        END, l_JOB_DTL_ID);

                	/*
                	 * Collect statistics for Standard Grouping Stage and History tables.
                	 */
                 	--COLLECT STATISTICS on p_erm_stg.stg_enctr_std_grp_hist; commented 2/24/2020
                 	--COLLECT STATISTICS on p_erm_int.enctr_std_grp_hist; commented 2/24/2020

                    --final Job History
        CALL D_ERM_MAC.P_ERM_JOB_HISTORY(:l_JOB_ID, :l_PRCS_NAME, :l_BUS_UNIT, :l_SCRIPT_NAME, 'C', l_activity_count, 0, 0,:l_RUN_ERROR_CD, :l_RUN_ERROR_MSG,  :l_JOB_ID);
        SET out_return_msg =  'Ended: '||to_char(l_job_id); /*
                 * VALIDATION
                 *
                SELECT * FROM std_grp_volatile std
                WHERE ENCTR_ID IN(728539161,724751802,727686619,726171001)
                ;
                */
                    /*
                     *

                       select count(*), STD_PTIENT_CLAS
                       from  D_ERM_IBV.enctr_hist eh
                       left join D_ERM_IBV.ENCTR_STD_GRP_HIST h on h.enctr_id = eh.enctr_id and h.name_space_cd = eh.name_space_cd and date'2017-08-31' between h.eff_from_dt and h.eff_thru_dt
                       where date'2017-08-31' between eh.eff_from_dt and eh.eff_thru_dt
                       and eh.name_space_cd = 'EPIC-CHICAGO'
                       group by STD_PTIENT_CLAS;



                      	select count(*), UEM_PAT_CLASS
                       from  D_ERM_IBV.d_encounter eh
                       where date'2017-08-31' between eh.eff_in_dt and eh.eff_out_dt
                              and eh.name_space_cd = 'EPIC-CHICAGO'
                       group by UEM_PAT_CLASS


                     */
    END;
