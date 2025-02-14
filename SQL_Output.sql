-- Generated SQL Queries
-- Source: /Users/saboor/Documents/Projects/Codes/Spark2SQL/SparkDataFrameExample.scala
-- Generated at: 2025-02-14T12:56:08.798218

SELECT srce_cd AS source code, srvc_call_cd AS service_call_code, plcmnt_opp_id AS placement_opp_id, to_date(event_ts AS event_dt, cm13, event_ts, event_ts, cmpn_id, offr_id, plcmnt_opp_id, srvc_call_cd, event_type_cd, event_score, mer_rank, srce_cd FROM `axp-lumid.dw_anon.eos_event_log_prop_us` WHERE offr_id IS NOT NULL AND cm13 IS NOT NULL AND event_ts IS NOT NULL;

SELECT indus_info_offr_id, MIN(ctgy_ns) as ctgy_na, MIN(subctgy_na) as subctgy_ns FROM `axp-lumid.dw_anon.gcor_ctgy` GROUP BY indus_info_offr_id;

SELECT DISTINCT offr_nm, offr_id FROM `axp-lumid.dw_anon.gcor_offr`;

SELECT sail_hist_id, offr_nm, MIN(offr_nm) as capt_na FROM `axp-lumid.dw_anon.ecp_email_mkt_chpon` GROUP BY sail_hist_id, offr_nm;

SELECT acct_cust_xref_id, acct_as_of_dt, MIN(acct_bus_unit_cd) as business_unit_cdf, COUNT(acct_bus_unit_cd) as ind FROM `axp-lumid.dw_anon.risk_pers_acct_hist` WHERE DATE_TRUNC('month', add_months(to_date(lit(config.startDate)), -1)) = acct_as_of_dt GROUP BY acct_cust_xref_id, acct_as_of_dt;

SELECT DISTINCT acct_credit_bureau_score AS fico_score, acct_cust_xref_id AS cust_xref_id, acct_tenure_mnths_ct AS account_tenure, acct_as_of_dt AS acct_as_of_dt, coll, acct_sub_prd_cd, acct_as_of_dt, acct_tenure_mnths_ct, acct_cust_xref_id, acct_credit_bureau_score FROM `axp-lumid.dw_anon.risk_pers_acct_hist`;

SELECT cust_xref_id, acct_as_of_dt, MIN(coll) as coll, MIN(acct_sub_prd_cd) as small_acct_sub_prd_cd, MIN(acct_as_of_dt) as first_acct_as_of_dt FROM `axp-lumid.dw_anon.risk_pers_acct_hist` GROUP BY cust_xref_id, acct_as_of_dt;

