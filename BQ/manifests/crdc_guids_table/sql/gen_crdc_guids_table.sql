  # Create a table of study, series and instance GUIDS for each instance
WITH
  # crdc_blob_manifests is the unformatted upload of the DCF output blob manifests
  # extract the data we need, being just the SOPInstanceUID, instance_guid
  instanceUID_and_instanceGUID AS (
  SELECT
    SPLIT(array_reverse(SPLIT(urls, '/'))[
    OFFSET
      (0)], '.dcm')[
  OFFSET
    (0)] AS SOPInstanceUID,
    guid AS instance_guid
  FROM
    `{project}.{dataset}.{blob_manifest}`),

  # add the StudyInstanceUID and SeriesInstanceUID
  allUIDs_and_instanceGUID AS (
  SELECT
    dcm.StudyInstanceUID,
    dcm.SeriesInstanceUID,
    instanceUID_and_instanceGUID.*
  FROM
    instanceUID_and_instanceGUID
  JOIN
    `{project}.{dataset}.{dicom_metadata}` dcm
  ON
    instanceUID_and_instanceGUID.SOPInstanceUID = dcm.SOPInstanceUID),

  # Now add the series guids
  allUIDs_and_series_instanceGUID AS (
  SELECT
    allUIDs_and_instanceGUID.*,
    series_bundles.series_guid
  FROM
    allUIDs_and_instanceGUID
  LEFT JOIN
    `{project}.{dataset}.{series_bundle_manifest}` series_bundles
  ON
    allUIDs_and_instanceGUID.SeriesInstanceUID = series_bundles.SeriesInstanceUID )

# Finally add the study guids
SELECT
  allUIDs_and_series_instanceGUID.*,
  study_bundles.study_guid
FROM
  allUIDs_and_series_instanceGUID
LEFT JOIN
  `{project}.{dataset}.{study_bundle_manifest}` study_bundles
ON
  allUIDs_and_series_instanceGUID.StudyInstanceUID = study_bundles.StudyInstanceUID
