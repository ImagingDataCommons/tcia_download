SELECT
  aux.* EXCEPT (CRDC_UUIDs),
  STRUCT(guids.study_guid AS study,
    guids.series_guid AS series,
    guids.instance_guid AS instance) AS CRDC_UUIDS
FROM
  {guids} AS guids
RIGHT JOIN
  {aux} AS aux
ON
  guids.SOPInstanceUID = aux.SOPInstanceUID