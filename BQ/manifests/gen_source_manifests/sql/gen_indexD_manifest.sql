SELECT
  CASE
    WHEN INDEXD_GUIDSs.SOPInstanceUID = "" THEN NULL
  ELSE
  INDEXD_GUIDSs.SOPInstanceUID
END
  AS GUID,
  TO_HEX(FROM_BASE64(MD5_Hash)) AS md5,
  Instance_Size AS size,
  "*" AS acl,
  REPLACE(GCS_URL,'https://storage.googleapis.com/','gs://') AS url
FROM
  `idc-dev-etl.idc_tcia_mvp_wave0.idc_tcia_auxilliary_metadata`
CROSS JOIN
  UNNEST(IDC_version) AS version;