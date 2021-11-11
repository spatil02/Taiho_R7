/*
CCDM SV mapping
Notes: Standard mapping to CCDM SV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     sv_data AS (
                --screening
SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                        "FolderName" ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISITDAT" ::date AS svstdtc,
                        "VISITDAT" ::date AS svendtc
                        from tas117_201."VISIT" v 
union all
---C1D1

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                        "FolderName" ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISITDAT" ::date AS svstdtc,
                        "VISITDAT" ::date AS svendtc
                        from tas117_201."DOVCD" d
union all
---C1D8

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                        "FolderName" ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISITDAT" ::date AS svstdtc,
                        "VISITDAT" ::date AS svendtc
                        from tas117_201."DOVCD8" d
union all
---C1D15

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                        "FolderName" ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISITDAT" ::date AS svstdtc,
                        "VISITDAT" ::date AS svendtc
                        from tas117_201."DOVCD15" d 
union all
---Cycle 2 Onwards

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                        "FolderName" ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        max("VISITDAT") ::date AS svstdtc,
                        max("VISITDAT") ::date AS svendtc
                        from tas117_201."DOVC2" d group by 1,2,3,4,5,6
union all
---Safety Follow Up

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                        "FolderName" ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISIT"::date AS svstdtc,
                        "VISIT" ::date AS svendtc
                        from tas117_201."DOVSFU" d 
                ),

     included_sites AS (
                  SELECT DISTINCT studyid,studyname,siteid,sitecountry,sitecountrycode,sitename,siteregion FROM site)

SELECT 
        /*KEY (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid)::text AS comprehendid, KEY*/
        sv.studyid::text AS studyid,
        --si.studyname::text AS studyname,
        sv.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.siteregion::text AS siteregion,
        si.sitecountry::text AS sitecountry,
        si.sitecountrycode::text AS sitecountrycode,
        sv.usubjid::text AS usubjid, 
        sv.visitnum::numeric AS visitnum,
        sv.visit::text AS visit,
        sv.visitseq::int AS visitseq,
        sv.svstdtc::date AS svstdtc,
        sv.svendtc::date AS svendtc
        /*KEY , (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid || '~' || sv.visitnum)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sv_data sv
JOIN included_subjects s ON (sv.studyid = s.studyid AND sv.siteid = s.siteid AND sv.usubjid = s.usubjid)
LEFT JOIN included_sites si ON (sv.studyid = si.studyid AND sv.siteid = si.siteid);

