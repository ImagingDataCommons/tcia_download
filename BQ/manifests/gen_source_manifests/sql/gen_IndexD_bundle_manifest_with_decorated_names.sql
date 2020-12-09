SELECT
  bundle_names,
  ids
FROM (
  SELECT
    1 AS sortby,
    CONCAT( aux.TCIA_API_CollectionID, "_series_",aux.StudyInstanceUID,"/",aux.SeriesInstanceUID) AS bundle_names,
    ARRAY_AGG(DISTINCT aux.CRDC_UUIDs.Instance ) AS ids
  FROM
    `{project}.{dataset}.{aux}` AS aux
  GROUP BY
    aux.StudyInstanceUID,
    aux.SeriesInstanceUID,
    aux.TCIA_API_CollectionID
  UNION ALL
  SELECT
    2 AS sortby,
    CONCAT( aux.TCIA_API_CollectionID, "_study_",aux.StudyInstanceUID) AS bundle_names,
    ARRAY_AGG(DISTINCT CONCAT(aux.TCIA_API_CollectionID,"_series_",aux.StudyInstanceUID,"/",aux.SeriesInstanceUID ) ) AS ids
  FROM
    `{project}.{dataset}.{aux}` AS aux
  GROUP BY
    aux.StudyInstanceUID,
    aux.TCIA_API_CollectionID)
ORDER BY
  sortby,
  bundle_names