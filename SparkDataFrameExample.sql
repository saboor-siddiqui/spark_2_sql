-- ================================================================
-- Generated SQL Queries
-- Source:    SparkDataFrameExample.scala
-- Generated: 2026-04-04T08:19:16
-- Provider:  anthropic / claude-sonnet-4-6
-- ================================================================

-- [LLM path] df
-- Source: df
SELECT
  *
FROM `axp-lumid.dw_anon.sales`;

-- [JAVA path] collibraPreHeaderIcmv
-- Source variable: collibraPreHeaderIcmv
SELECT
  em_resp_model_id AS email_model_id,
  tax_rt AS tax_rate,
  em_resp_model_id AS email_model_id,
  offr_total_cost_am AS total_cost,
  offer_nm_incentive,
  incentive_buckets,
  collibra_model_id,
  roi_cutoff,
  crc_channel_cost,
  resp_model_id,
  myca_resp_model_id,
  crc_resp_model_id,
  start_date,
  end_date
FROM `axp-lumid.dw_anon.collibra_header_icmv`
WHERE
  crc_channel_cost > 1000;

-- [JAVA path] cspModelScoreCm11
-- Source variable: cspModelScoreCm11
SELECT
  cm11,
  coll_outp_vert,
  coll_seere,
  count(cm11) AS total_customers
FROM `axp-lumid.dw_anon.csp_model_score_cm11`
INNER JOIN `axp-lumid.dw_anon.gstar_transactions`
  ON csp_model_score_cm11.cm13 = gstar_transactions.cm13
GROUP BY
  cm11,
  coll_outp_vert,
  coll_seere
ORDER BY
  total_customers DESC;

-- [JAVA path] cms_campaign_info_avg
-- Source variable: cms_campaign_info_avg
SELECT
  cm11,
  mbr_key,
  mkt_cd,
  avg(roi_cutoff) AS avg_roi_cutoff
FROM `axp-lumid.dw_anon.cms_campaign_info`
INNER JOIN axp-lumid.dw_anon.csp_model_score_cm11
  ON cms_campaign_info.cm13csp_model_score_cm11.cm13
GROUP BY
  cm11,
  mbr_key,
  mkt_cd
ORDER BY
  avg_roi_cutoff DESC;
