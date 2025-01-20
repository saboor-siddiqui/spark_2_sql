-- Generated SQL Queries
-- Source: /Users/saboor/Documents/Projects/Codes/Spark2SQL/SparkDataFrameExample.scala
-- Generated at: 2025-01-20T09:54:37.635436

SELECT em_resp_model_id AS email_model_id, tax_rt AS tax_rate, em_resp_model_id AS email_model_id, offr_total_cost_am AS total_cost, offer_nm_incentive, incentive_buckets, collibra_model_id, roi_cutoff, crc_channel_cost, resp_model_id, myca_resp_model_id, crc_resp_model_id, start_date, end_date FROM `axp-lumid.dw_anon.collibra_header_icmv` WHERE crc_channel_cost > 1000;

SELECT coll outp vert, coll seere, offer_nm_incentive, incentive_buckets, collibra_model_id, roi_cutoff, cm11, cm11, coll outp vert, coll seere, count(cm11) as total_customers FROM `axp-lumid.dw_anon.csp_model_score_cm11` INNER JOIN `axp-lumid.dw_anon.gstar_transactions` ON csp_model_score_cm11.cm13=gstar_transactions.cm13 GROUP BY cm11, coll outp vert, coll seere ORDER BY total_customers desc;

SELECT mbr_key, abr_id, aud_creat_ts, strt_ts, mkt_cd, key_type, cm13, roi_cutoff, cm11, mbr_key, mkt_cd, avg(roi_cutoff) as avg_roi_cutoff FROM `axp-lumid.dw_anon.cms_campaign_info` INNER JOIN `axp-lumid.dw_anon.csp_model_score_cm11` ON cms_campaign_info.cm13=csp_model_score_cm11.cm13 GROUP BY cm11, mbr_key, mkt_cd ORDER BY total_customers desc;

