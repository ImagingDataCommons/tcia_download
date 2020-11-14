SELECT
  aux.*
FROM (
  SELECT
    * EXCEPT (Study,
      Series,
      Instance),
    STRUCT(Study AS Study,
      Series AS Series,
      Instance AS Instance) AS CRDC_UUIDs
  FROM (
    SELECT
      DISTINCT x.* EXCEPT (CRDC_UUIDs),
      x.CRDC_UUIDs.Study AS Study,
      x.CRDC_UUIDs.Series AS Series,
      g.guid AS Instance
    FROM
      `idc-dev-etl.idc_tcia_mvp_wave1.idc_tcia_auxilliary_metadata_null_guids` AS x
    LEFT JOIN
      `idc-dev-etl.idc_tcia_mvp_wave1.idc_tcia_crdc_guids` AS g
    ON
      x.GCS_URL = g.urls)) AS aux
JOIN
  `idc-dev-etl.idc_tcia_mvp_wave1.idc_tcia_dicom_metadata` AS dcm
ON
  aux.SOPInstanceUID = dcm.SOPInstanceUID