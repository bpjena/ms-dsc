WITH weekly_schedule_date AS
(
--snapshot dates only matching dates in schedule get filtered
SELECT
    MAX(snap_date) AS snap_date
FROM sfdc.pipeline p
JOIN sfdc_op.schedule s ON date_part(month, p.snap_date) = s.month_num
                       AND date_part(day, p.snap_date) = s.date_num
),

sfdc_op_history AS
(
SELECT
   oprt.snap_date
   , sc3.fiscal_yearquarter               AS snap_quarter_year
   , a.case_safe_account_id__c            AS case_safe_account_id
   , oprt.case_safe_opportunity_id__c     AS case_safe_opportunity_id
   , a.account_quality__c                 AS account_tier
   , a.type                               AS account_type
   , a."name"                             AS account_name
   , oprt."name"                          AS opportunity_name
   , oprt.highest_stage_value_achieved__c AS highest_stage_value_achieved
   , oprt.type                            AS type
   , a.account_owner_name__c              AS account_owner
   , oprt.o__c                            AS opportunity_owner
   , oprt.stagename                       AS stage
   , oprt.qualified_date__c               AS qualified_date
   , oprt.closedate                       AS close_date
   , oprt.forecastcategory                AS forecast_category
   , oprt.amount
   , oprt.net_subscription_acv__c         AS net_subscription_acv
   , oprt.subscription_discount__c        AS subscription_logs_metrics_discount_del
   , oprt.opportunity_owner_function__c   AS function
   , a.account_sub_segment__c             AS segment
   , a.region__c                          AS geo
   , oprt.opportunity_owner_territory__c  AS opportunity_owner_geo
   , oprt.opportunity_owner_segment__c    AS opportunity_owner_segment
   , a.sub_region__c                      AS area
   , oprt.createddate::date               AS created_date
   , a.df_number_of_employee__c           AS employees
   , a.billingpostalcode                  AS billing_zip_postal_code
   , a.billingcity                        AS billing_city
   , a.billingstate                       AS billing_state_province
   , a.billingcountry                     AS billing_country
   , oprt.credited_sdr_name__c            AS credited_sdr_obr
   , CASE WHEN oprt.from_mql__c = true THEN 1
	      WHEN oprt.from_mql__c = false THEN 0 END AS from_marketing
   , oprt.opportunity_source__c           AS opportunity_source
   , oprt.opportunity_source_detail__c    AS opportunity_source_detail
   , a.abm_saa_date__c                    AS abm_saa_date
   , a.abm_saa_date__c                    AS abm_sqa_date
   , oprt.sourced_by__c                   AS sourced_by
   , CASE WHEN oprt.partner_involved__c = true THEN 1
	      WHEN oprt.partner_involved__c = false THEN 0 END AS partner_involved
   , a.aws_registration_status__c         AS aws_registration_status
   , CASE WHEN oprt.aspin_deal__c = true THEN 1
	      WHEN oprt.aspin_deal__c = false THEN 0 END AS aspin
   , oprt.aws_registered_date__c          AS aws_submitted_date
   , oprt.registration_type__c            AS transaction_type
   , oprt.agreement_type__c               AS agreement_type
   , CASE WHEN a."name" NOT LIKE '%Sensu]%' THEN 'F'
	                                        ELSE 'T' END AS account_name_sensu
   , CASE WHEN a."name" NOT LIKE '%DF Labs%' THEN 'F'
                                             ELSE 'T' END AS account_name_df_labs
   , CASE WHEN oprt."name" NOT LIKE '%Sensu%' OR oprt."name" LIKE '%Sensus%' THEN 'F'
                                            ELSE 'T' END AS opportunity_name_sensu
   , CASE WHEN oprt."name" NOT LIKE '%DF Labs%' THEN 'F'
                                                ELSE 'T' END AS opportunity_name_df_labs
   , sc2.fiscal_yearquarter                  AS created_quarter
   , sc.fiscal_yearquarter                   AS close_quarter
   , current_timestamp                       AS dw_load_dt
FROM sfdc.pipeline oprt
JOIN ab_sfdc.recordtype r     ON r.id = oprt.recordtypeid
JOIN ab_sfdc.account a        ON a.case_safe_account_id__c = oprt.accountid
JOIN audit.sumo_calendar sc   ON oprt.closedate = sc.date
JOIN audit.sumo_calendar sc2  ON oprt.createddate::date = sc2.date
JOIN weekly_schedule_date wsd ON oprt.snap_date = wsd.snap_date
JOIN audit.sumo_calendar sc3  ON oprt.snap_date = sc3.date
WHERE 1=1
	--and oprt.snap_date = '2022-10-22'
    AND SUBSTRING(oprt.stagename,1,1) != 'C' --closed stage filter out
    AND oprt.createddate <= CURRENT_DATE
    AND oprt.closedate >= sc3.fiscal_month_start_date
    AND oprt.closedate < dateadd(YEAR, 1, sc3.fiscal_month_start_date)::date --8/1/2022 to 8/1/2023
    AND (oprt.edition_type__c NOT IN ('Sensu','IncMan') OR oprt.edition_type__c IS NULL)
    AND (oprt."name" NOT LIKE '%Sensu%' OR oprt."name" LIKE '%Sensus%')
    AND	oprt."name" NOT LIKE '%DF Labs%'
    AND a."name" NOT LIKE '%Sensu]%'
    AND	a."name" NOT LIKE '%DF Labs%'
    AND oprt.opportunity_owner_segment__c IS NOT NULL
    AND r.name NOT IN ('Renewal','Non-Recurring ACV','Overages')
 ),

sfdc_op_by_type AS
(
SELECT
    ROUND((opl.snap_date - sc.fiscal_month_start_date)/7::decimal(38,10)) AS ref_week
    , sc.fiscal_month_start_date AS quarter_start
    , opl.case_safe_opportunity_id
    , opl.snap_date
    , sc.fiscal_yearquarter as close_quarter_year
    , opl."type" AS type
    , opl.stage  AS stage
    , opl.amount
    , opl.net_subscription_acv
    , opl.opportunity_name
    , opl.opportunity_owner_geo
    , opl.opportunity_owner_segment
    , opl.close_date
    , CURRENT_TIMESTAMP AS dw_load_dt
FROM sfdc_op_history opl
JOIN audit.sumo_calendar sc ON opl.close_date = sc."date"
WHERE opl."type" IN ('New','New Logo','Cross-Sell','Upgrade')
),

sfdc_op_plan AS
(
SELECT
 quarter_year
 , ref_week
 , snap_date
 , type
 , type_d
 , geo
 , segment
 , duplicated_count
 , round(sum_of_amount,4) AS sum_of_amount
 , sum_of_acv
 , sum_of_numops
 , CASE WHEN type_d = 'new-logo' THEN pcw.new_logo
 	    WHEN type_d = 'expansion' THEN pcw.expansion END AS plan_c_or_w
 , round((sum_of_amount /  (CASE WHEN type_d = 'new-logo' THEN pcw.new_logo
 	                             WHEN type_d = 'expansion' THEN pcw.expansion END)::decimal(38,10)),4) AS coverage_by_amount
 , round((sum_of_acv /  (CASE WHEN type_d = 'new-logo' THEN pcw.new_logo
 	                          WHEN type_d = 'expansion' THEN pcw.expansion END)::decimal(38,10)),4) AS coverage_by_acv
 , CURRENT_TIMESTAMP AS dw_load_dt
FROM (
 SELECT
 (split_part(close_quarter_year,'-',1)||'-'||
   CASE WHEN split_part(close_quarter_year,'-',2) = '01' THEN 'Q1'
 	    WHEN split_part(close_quarter_year,'-',2) = '02' THEN 'Q2'
 	    WHEN split_part(close_quarter_year,'-',2) = '03' THEN 'Q3'
 	    WHEN split_part(close_quarter_year,'-',2) = '04' THEN 'Q4' END)::varchar quarter_year
 , ref_week
 , snap_date
 , type
 , CASE WHEN type IN ('New Logo','New') THEN 'new-logo'
        WHEN type IN ('Cross-Sell','Upgrade') THEN 'expansion' END type_d
 , opportunity_owner_geo AS geo
 , opportunity_owner_segment AS segment
 , ROW_NUMBER() OVER (PARTITION BY close_quarter_year, type, opportunity_owner_geo,
 					opportunity_owner_segment, ref_week ORDER BY close_quarter_year, snap_date DESC) AS duplicated_count
 , SUM(amount) AS sum_of_amount
 , SUM(net_subscription_acv) AS sum_of_acv
 , COUNT(case_safe_opportunity_id) AS sum_of_numops
 FROM sfdc_op_by_type opt
 GROUP BY
    close_quarter_year,
    2,3,6,7,
    type
	 ) stg
JOIN sfdc_op.plan_closed_won pcw ON stg.quarter_year = pcw.fiscal_quarter
WHERE stg.duplicated_count = 1
),

sfdc_op_coverage AS
(
SELECT
 ref_week,
 type,
 geo,
 segment,
 Q3_2021,
 Q4_2021,
 Q1_2022,
 Q2_2022,
 Q3_2022,
 Q4_2022,
 Q1_2023,
 Q2_2023,
 Q3_2023,
 Q4_2023,
 Q1_2024,
 Q2_2024,
 Q3_2024,
 ROUND((COALESCE(Q3_2021,0) +COALESCE(Q4_2021,0) +COALESCE(Q1_2022,0)
        +COALESCE(Q2_2022,0) +COALESCE(Q3_2022,0) +COALESCE(Q4_2022,0)
        +COALESCE(Q1_2023,0) +COALESCE(Q2_2023,0) +COALESCE(Q3_2023,0)) /
        (CASE WHEN (Q3_2021_flag+Q4_2021_flag+Q1_2022_flag
                    +Q2_2022_flag+Q3_2022_flag+Q4_2022_flag
                    +Q1_2023_flag+Q2_2023_flag+Q3_2023_flag) = 0 THEN NULL
            ELSE (Q3_2021_flag+Q4_2021_flag+Q1_2022_flag
                    +Q2_2022_flag+Q3_2022_flag+Q4_2022_flag
                    +Q1_2023_flag+Q2_2023_flag+Q3_2023_flag) END)::decimal(38,10),4) AS baseline_coverage
FROM
(
 SELECT
   ref_week,
   type,
   geo,
   segment,
   Q3_2021,
   Q4_2021,
   Q1_2022,
   Q2_2022,
   Q3_2022,
   Q4_2022,
   Q1_2023,
   Q2_2023,
   Q3_2023,
   Q4_2023,
   Q1_2024,
   Q2_2024,
   Q3_2024,
   CASE WHEN Q3_2021 isnull THEN 0 ELSE 1 END Q3_2021_flag,
   CASE WHEN Q4_2021 isnull THEN 0 ELSE 1 END Q4_2021_flag,
   CASE WHEN Q1_2022 isnull THEN 0 ELSE 1 END Q1_2022_flag,
   CASE WHEN Q2_2022 isnull THEN 0 ELSE 1 END Q2_2022_flag,
   CASE WHEN Q3_2022 isnull THEN 0 ELSE 1 END Q3_2022_flag,
   CASE WHEN Q4_2022 isnull THEN 0 ELSE 1 END Q4_2022_flag,
   CASE WHEN Q1_2023 isnull THEN 0 ELSE 1 END Q1_2023_flag,
   CASE WHEN Q2_2023 isnull THEN 0 ELSE 1 END Q2_2023_flag,
   CASE WHEN Q3_2023 isnull THEN 0 ELSE 1 END Q3_2023_flag
   FROM
        (
        SELECT
         ref_week,
         type_d AS type,
         geo,
         segment,
         ROUND(AVG(CASE WHEN quarter_year = '2021-Q3' THEN coverage_by_amount END),4) AS Q3_2021,
         ROUND(AVG(CASE WHEN quarter_year = '2021-Q4' THEN coverage_by_amount END),4) AS Q4_2021,
         ROUND(AVG(CASE WHEN quarter_year = '2022-Q1' THEN coverage_by_amount END),4) AS Q1_2022,
         ROUND(AVG(CASE WHEN quarter_year = '2022-Q2' THEN coverage_by_amount END),4) AS Q2_2022,
         ROUND(AVG(CASE WHEN quarter_year = '2022-Q3' THEN coverage_by_amount END),4) AS Q3_2022,
         ROUND(AVG(CASE WHEN quarter_year = '2022-Q4' THEN coverage_by_amount END),4) AS Q4_2022,
         ROUND(AVG(CASE WHEN quarter_year = '2023-Q1' THEN coverage_by_amount END),4) AS Q1_2023,
         ROUND(AVG(CASE WHEN quarter_year = '2023-Q2' THEN coverage_by_amount END),4) AS Q2_2023,
         ROUND(AVG(CASE WHEN quarter_year = '2023-Q3' THEN coverage_by_acv END),4)    AS Q3_2023,
         ROUND(AVG(CASE WHEN quarter_year = '2023-Q4' THEN coverage_by_acv END),4)    AS Q4_2023,
         ROUND(AVG(CASE WHEN quarter_year = '2024-Q1' THEN coverage_by_acv END),4)    AS Q1_2024,
         ROUND(AVG(CASE WHEN quarter_year = '2024-Q2' THEN coverage_by_acv END),4)    AS Q2_2024,
         ROUND(AVG(CASE WHEN quarter_year = '2024-Q3' THEN coverage_by_acv END),4)    AS Q3_2024
        FROM sfdc_op_plan oppc
        GROUP BY 1,2,3,4
        --order by 3,4,1,2
        ) stg
) fin
)

SELECT * FROM sfdc_op_coverage
