/*Mapping script for Missing Pages
Table name : ckpi.ckpi_missing_stream_pages
Project name : Taiho*/

drop table if exists ckpi."ckpi_missing_stream_pages_new";

create table ckpi."ckpi_missing_stream_pages_new" as 
with study_data as
(select study.studyid as "Study" from cqs.study ),
site_data as
(select site.studyid as "Study",site.siteid as "Site",site.sitename as "Site Name" from cqs.site ),
subject_data as
(select subject.studyid as "Study", subject.siteid as "Site", subject.usubjid as "Subject" from cqs.subject ),
cohort_data as
(select dm.studyid as "Study",dm.siteid as "Site",dm.usubjid as "Subject", dm.arm as "Cohort"
 from cqs.dm),
rand_date_data as
(select ds.studyid as "Study",ds.siteid as "Site",ds.usubjid as "Subject",ds.dsstdtc as "Enrollment Date"
 from cqs.ds
 where lower(trim(dscat)) = 'enrollment'and lower(trim(dsterm)) = 'enrolled'),
wdrl_date_data as
(select *,ds.studyid as "Study",ds.siteid as "Site",ds.usubjid as "Subject",ds.dsstdtc as "Withdrawal Date"
 from cqs.ds
 WHERE dsseq > 4.01 AND dsseq < 5.0),
Screen_Failed_Date as
(select ds.studyid as "Study",ds.siteid as "Site",ds.usubjid as "Subject",ds.dsstdtc as "Screen Failed Date"
from cqs.ds
where lower(trim(dsterm)) = 'failed screen'),
DTH_data as
(
select
'TAS120_201' as studyid,
"SiteNumber" as siteid ,
"Subject" as usubjid,
"DTHDAT" as "Date of Death"
from tas120_201."DTH"
union
select
'TAS120_202' as studyid,
"SiteNumber" as siteid ,
"Subject" as usubjid,
"DTHDAT" as "Date of Death"
from tas120_202."DTH"
union
select
'TAS0612_101' as studyid,
'TAS0612_101_'||split_part("SiteNumber",'_',2) as siteid ,
"Subject" as usubjid,
"DTHDAT" as "Date of Death"
from tas0612_101."DTH"
union
select
'TAS3681_101_DOSE_ESC' as studyid,
"SiteNumber" as siteid ,
"Subject" as usubjid,
"DTHDAT" as "Date of Death"
from tas3681_101."DTH"
where "Subject" in (select distinct usubjid from cqs.subject where studyid = 'TAS3681_101_DOSE_ESC')
union
select
'TAS3681_101_DOSE_EXP' as studyid,
"SiteNumber" as siteid ,
"Subject" as usubjid,
"DTHDAT" as "Date of Death"
from tas3681_101."DTH"
where "Subject" in (select distinct usubjid from cqs.subject where studyid = 'TAS3681_101_DOSE_EXP')
UNION
select
'TAS120_203' as studyid,
'TAS120_203_'||split_part("SiteNumber",'_',2) as siteid ,
"Subject" as usubjid,
"DTHDAT" as "Date of Death"
from tas120_203."DTH"
UNION
select
'TAS120_204' as studyid,
'TAS120_204_'||split_part("SiteNumber",'_',2) as siteid ,
"Subject" as usubjid,
"DTHDAT" as "Date of Death"
from tas120_204."DTH"
UNION
select
'TAS117_201' as studyid,
'TAS117_201_'||split_part("SiteNumber",'_',2) as siteid ,
"Subject" as usubjid,
"DTHDAT" as "Date of Death"
from TAS117_201."DTH"
UNION
select
'TAS2940_101' as studyid,
'TAS2940_101_'||split_part("SiteNumber",'_',2) as siteid ,
"Subject" as usubjid,
"DTHDAT" as "Date of Death"
from TAS2940_101."DTH"
),
vst_no_n_date_data as
(select sv.studyid as "Study", sv.visitnum as "Visit Number",sv.visit as "Trial Visit Name"
from cqs.tv sv
),
days_on_study_data as
(select distinct rsd.studyid as "Study",rsd.siteid as "Site",rsd.usubjid as "Subject",
rsd.totalsubjectdays as "Days on Study"
from cqs.rpt_subject_days rsd),
pcm_pages_data as
(select
'TAS0612_101'::text as "Study",
concat('TAS0612_101','_',left( sitename::text,3))as "Site",
"subjectname"::text as "Subject",
trim(REGEXP_REPLACE(REGEXP_REPLACE("foldername",'\s\([0-9]\)',''),'[0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: TEXT as "Visit" ,
"pagesexpected_total"::text as "Pages Expected",
"pagesentered"::text as "Completed Pages",
"formname" as "Form"
from tas0612_101.stream_page_status pagesentered
--where "pagesexpected_todate"::numeric=1
--group by sitename, "subjectname",foldername,"formname"
UNION ALL
select
'TAS120_201'::text as "Study",
concat('TAS120_201','_',left( sitename::text,3))as "Site",
"subjectname"::text as "Subject",
trim(REGEXP_REPLACE(REGEXP_REPLACE("foldername",'\s\([0-9]\)',''),'[0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: TEXT as "Visit" ,
"pagesexpected_total"::text as "Pages Expected",
"pagesentered"::text as "Completed Pages",
"formname" as "Form"
from tas120_201.stream_page_status pagesentered
--where "pagesexpected_todate"::numeric=1
--group by sitename, "subjectname",foldername,"formname"
UNION ALL
select
'TAS120_202'::text as "Study",
concat('TAS120_202','_',left( sitename::text,3))as "Site",
"subjectname"::text as "Subject",
trim(REGEXP_REPLACE(REGEXP_REPLACE("foldername",'\s\([0-9]\)',''),'[0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: TEXT as "Visit" ,
"pagesexpected_total"::text as "Pages Expected",
"pagesentered"::text as "Completed Pages",
"formname" as "Form"
from tas120_202.stream_page_status pagesentered
--where "pagesexpected_todate"::numeric=1
--group by sitename, "subjectname",foldername,"formname"
UNION ALL
select
'TAS3681_101_DOSE_ESC'::text as "Study",
concat('TAS3681101','_',left( sitename::text,3))as "Site",
"subjectname"::text as "Subject",
trim(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("foldername",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),'[0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: TEXT AS Visit, 
"pagesexpected_total"::text as "Pages Expected",
"pagesentered"::text as "Completed Pages",
"formname" as "Form"
from --tas3681_101."IE" i JOIN
tas3681_101.stream_page_status sps
--where "pagesexpected_todate"::numeric=1
--ON i."Subject" = sps."subjectname"
--where "IERANDY" != 'Expansion'
--group by sitename, "subjectname",foldername,"formname"
UNION ALL
select
'TAS3681_101_DOSE_EXP'::text as "Study",
concat('TAS3681101','_',left( sitename::text,3))as "Site",
"subjectname"::text as "Subject",
trim(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("foldername",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),'[0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: TEXT AS Visit, 
"pagesexpected_total"::text as "Pages Expected",
"pagesentered"::text as "Completed Pages",
"formname" as "Form"
from --tas3681_101."IE" i JOIN
tas3681_101.stream_page_status sps --ON i."Subject" = sps."subjectname"
--where "pagesexpected_todate"::numeric=1
--where "IERANDY" = 'Expansion'
--group by sitename, "subjectname",foldername,"formname"
UNION ALL
select
'TAS120_203'::text as "Study",
concat('TAS120_203','_',left( sitename::text,3))as "Site",
"subjectname"::text as "Subject",
trim(REGEXP_REPLACE(REGEXP_REPLACE("foldername",'\s\([0-9]\)',''),'[0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: TEXT as "Visit" ,
"pagesexpected_total"::text as "Pages Expected",
"pagesentered"::text as "Completed Pages",
"formname" as "Form"
from tas120_203.stream_page_status pagesentered
--where "pagesexpected_todate"::numeric=1
--group by sitename, "subjectname",foldername,"formname"
UNION ALL
select
'TAS120_204'::text as "Study",
concat('TAS120_204','_',left( sitename::text,3))as "Site",
"subjectname"::text as "Subject",
trim(REGEXP_REPLACE(REGEXP_REPLACE("foldername",'\s\([0-9]\)',''),'[0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: TEXT as "Visit" ,
"pagesexpected_total"::text as "Pages Expected",
"pagesentered"::text as "Completed Pages",
"formname" as "Form"
from tas120_204.stream_page_status pagesentered
--where "pagesexpected_todate"::numeric=1
--group by sitename, "subjectname",foldername,"formname"
UNION ALL
select
'TAS117_201'::text as "Study",
concat('TAS117_201','_',left( sitename::text,3))as "Site",
"subjectname"::text as "Subject",
trim(REGEXP_REPLACE(REGEXP_REPLACE("foldername",'\s\([0-9]\)',''),'[0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: TEXT as "Visit" ,
"pagesexpected_total"::text as "Pages Expected",
"pagesentered"::text as "Completed Pages",
"formname" as "Form"
from TAS117_201.stream_page_status pagesentered
--where "pagesexpected_todate"::numeric=1
--group by sitename, "subjectname",foldername,"formname"
UNION ALL
select
'TAS2940_101'::text as "Study",
concat('TAS2940_101','_',left( sitename::text,3))as "Site",
"subjectname"::text as "Subject",
trim(REGEXP_REPLACE(REGEXP_REPLACE("foldername",'\s\([0-9]\)',''),'[0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: TEXT as "Visit" ,
"pagesexpected_total"::text as "Pages Expected",
"pagesentered"::text as "Completed Pages",
"formname" as "Form"
from TAS2940_101.stream_page_status pagesentered
--where "pagesexpected_todate"::numeric=1
--group by sitename, "subjectname",foldername,"formname"
),
pcm_pages as (
select
"Study",
"Site",
"Subject",
"Visit",
"Pages Expected",
"Completed Pages",
"Form",
(case
when --"Pages Expected"::text = '1'and 
"Completed Pages"::text = '1' then 0
when --"Pages Expected"::text = '1'and 
"Completed Pages"::text = '0' then 1
else null end)::int as "Missing Pages"
from "pcm_pages_data") ,
missing_pages as
(
select
study."Study",
dm."Cohort",
site."Site",
site."Site Name",
pcm."Subject",
rand."Enrollment Date",
wdrl."Withdrawal Date",
screen_failed."Screen Failed Date",
dth."Date of Death",
pcm."Pages Expected",
pcm."Completed Pages",
pcm."Missing Pages",
pcm."Visit" as "Visit",
pcm."Form",
NULL AS "Date",
dsd."Days on Study",
vst."Visit Number",
vst."Trial Visit Name" as "Visit1"
from pcm_pages pcm
left outer join days_on_study_data dsd on
pcm."Study" = dsd."Study"
and pcm."Site" = dsd."Site"
and pcm."Subject" = dsd."Subject"
left outer join vst_no_n_date_data vst on
pcm."Study" = vst."Study"
and lower(trim(pcm."Visit")) =  lower(trim(vst."Trial Visit Name"))
left outer join wdrl_date_data wdrl on
pcm."Study" = wdrl."Study"
and pcm."Site" = wdrl."Site"
and pcm."Subject" = wdrl."Subject"
left outer join rand_date_data rand on
pcm."Study" = rand."Study"
and pcm."Site" = rand."Site"
and pcm."Subject" = rand."Subject"
left outer join Screen_Failed_Date screen_failed on
pcm."Study" = screen_failed."Study"
and pcm."Site" = screen_failed."Site"
and pcm."Subject" = screen_failed."Subject"
inner join site_data site on
pcm."Study" = site."Study"
and pcm."Site" = site."Site"
inner join cohort_data dm on
pcm."Study" = dm."Study"
and pcm."Site" = dm."Site"
and pcm."Subject" = dm."Subject"
inner join study_data study on
study."Study" = site."Study"
left outer join DTH_data dth on
pcm."Study" = dth.studyid
and pcm."Site" = dth.siteid
and pcm."Subject" = dth.usubjid
order by study."Study",dm."Cohort",site."Site",pcm."Subject",vst."Visit Number",pcm."Visit"
)
select
"Study",
"Cohort",
"Site",
"Site Name",
"Subject",
"Enrollment Date" as "Randomization Date",
"Withdrawal Date",
"Screen Failed Date",
"Pages Expected"::numeric as "Pages Expected",
"Completed Pages"::numeric as "Pages Completed" ,
"Missing Pages"::numeric,
"Date" as "Visit Date",
"Form" as "Page",
"Days on Study",
"Visit Number",
"Visit",
"Visit1"
from missing_pages
where  "Form" not in ('IRT Load place holder','CTCAE Grading');
--WHERE  "Subject" = '101-005-P1'
--where "Visit1" = "Visit"

drop table if exists ckpi."ckpi_missing_stream_pages_orig";

alter table if exists ckpi."ckpi_missing_stream_pages" rename to "ckpi_missing_stream_pages_orig";

alter table if exists ckpi."ckpi_missing_stream_pages_new" rename to "ckpi_missing_stream_pages";	

--ALTER TABLE ckpi."ckpi_missing_stream_pages" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE ckpi."ckpi_missing_stream_pages_orig" OWNER TO "taiho-dev-app-clinical-master-write";
