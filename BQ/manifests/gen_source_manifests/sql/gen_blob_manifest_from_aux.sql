select CRDC_UUIDS.instance as GUID, TO_HEX(FROM_BASE64(MD5_Hash)) as md5, Instance_size as size, "*" as acl, GCS_URL as url
from {aux}
where IDC_Webapp_CollectionID = "{collection}"