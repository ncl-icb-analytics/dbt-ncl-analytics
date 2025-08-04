-- Staging model for epd_primary_care.MedsHeader
-- Source: "DATA_LAKE"."EPD_PRIMARY_CARE"
{% if source.get('description') %}
-- Description: Primary care medications and prescribing data
{% endif %}

select
    "DatSerVer" as datserver,
    "OrgIdProvider" as orgidprovider,
    "RPStartDate" as rpstartdate,
    "RPEndDate" as rpenddate,
    "ReceivedDate" as receiveddate,
    "FileType" as filetype,
    "TotalRecords" as totalrecords,
    "UniqSubmissionID" as uniqsubmissionid,
    "dmicProcessedPeriod" as dmicprocessedperiod,
    "Unique_MonthID" as unique_monthid,
    "dmicImportLogId" as dmicimportlogid,
    "dmicSystemId" as dmicsystemid,
    "dmicDateAdded" as dmicdateadded
from {{ source('epd_primary_care', 'MedsHeader') }}
