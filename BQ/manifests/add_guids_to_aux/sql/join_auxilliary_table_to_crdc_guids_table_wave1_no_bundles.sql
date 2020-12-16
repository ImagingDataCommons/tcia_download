SELECT
  aux.* EXCEPT (CRDC_UUIDs),
  STRUCT("" AS study,
    "" AS series,
    guids.instance_guid AS instance) AS CRDC_UUIDS
FROM
  {guids} AS guids
RIGHT JOIN
  {aux} AS aux
ON
  guids.SOPInstanceUID = aux.SOPInstanceUID