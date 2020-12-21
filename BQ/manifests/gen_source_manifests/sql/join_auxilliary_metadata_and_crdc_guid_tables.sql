SELECT
  x.* EXCEPT (CRDC_UUIDs),
  STRUCT (x.CRDC_UUIDs.Study AS Study,
    x.CRDC_UUIDs.Series AS Series,
    g.guid AS Instance ) AS CRDC_UUIDs
FROM
  `idc-dev-etl.idc_tcia_mvp_wave0.idc_tcia_auxilliary_metadata_null_guids` AS x
JOIN
  `idc-dev-etl.idc_tcia_mvp_wave0.idc_tcia_crdc_guids` AS g
ON
  x.GCS_URL = g.urls