-- Generated SQL Queries
-- Source: /Users/saboor/Documents/Projects/Codes/Spark2SQL/SparkDataFrameExample.scala
-- Generated at: 2025-02-13T20:23:36.481665

SELECT DISTINCT col1, col2 FROM `axp-lumid.dw_anon.axp-lumid.dw_anon.cmdl_card_main` WHERE TRIM(ctry_cd) = "US" && cif = "y" && cust_cref_id IS NOT NULL;

SELECT indus_info_offr_id, ctgy_ns,  FROM `axp-lumid.dw_anon.axp-lumid.dw_anon.gcor_ctgy` GROUP BY indus_info_offr_id, ctgy_ns;

SELECT DISTINCT col1, col2 FROM `axp-lumid.dw_anon.axp-lumid.dw_anon.gcor_offr`;

SELECT sail_hist_id, offr_nm,  FROM `axp-lumid.dw_anon.axp-lumid.dw_anon.ecp_email_mkt_chpon` GROUP BY sail_hist_id, offr_nm;

SELECT acct_cust_xref_id, acct_as_of_dt,  FROM `axp-lumid.dw_anon.axp-lumid.dw_anon.risk_pers_acct_hist` WHERE DATE_TRUNC('month', add_months(to_date(lit(config.startDate)), -1)) = acct_as_of_dt GROUP BY acct_cust_xref_id, acct_as_of_dt;

