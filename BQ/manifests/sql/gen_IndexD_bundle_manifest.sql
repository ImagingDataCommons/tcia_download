SELECT
  bundle_names,
  ids
FROM (
  SELECT
    1 AS orderby,
    dicom.SeriesInstanceUID AS bundle_names,
    ARRAY_AGG(DISTINCT aux.CRDC_UUIDs.Instance ) AS ids
  FROM
    `idc-dev-etl.idc_tcia_mvp_wave0.idc_tcia_dicom_metadata` AS dicom
  JOIN
    `idc-dev-etl.idc_tcia_mvp_wave0.idc_tcia_auxilliary_metadata` AS aux
  ON
    dicom.SOPInstanceUID = aux.SOPInstanceUID
  GROUP BY
    dicom.SeriesInstanceUID
  UNION ALL
  SELECT
    2 AS orderby,
    dicom.StudyInstanceUID AS bundle_names,
    ARRAY_AGG(DISTINCT dicom.SeriesInstanceUID ) AS ids
  FROM
    `idc-dev-etl.idc_tcia_mvp_wave0.idc_tcia_dicom_metadata` AS dicom
  GROUP BY
    dicom.StudyInstanceUID ) bundles
ORDER BY
  orderby