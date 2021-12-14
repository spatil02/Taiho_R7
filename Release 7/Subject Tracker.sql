with ds_data_enrolled as
(
select
studyid ,
siteid,
usubjid,
max(dsstdtc) as "Randomization or Screen Fail Date" --subject 601-005 has two dates
from cqs.ds
where dsterm in ('Enrolled','Failed Screen')
group by 1,2,3
),

 ds_data_completed as --35
(
select
studyid ,
siteid,
usubjid,
max(dsstdtc) as "EOS Date"
from cqs.ds
where dsterm in ('Completed', 'Withdrawn','Discontinued before Treatment')
group by 1,2,3

),

subject_status as
(
select distinct studyid,siteid,usubjid,status from cqs.subject
),

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

union

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

sv_eot_date as
(
select
studyid ,
siteid,
usubjid,
dsstdtc as "EOT Date"
from cqs.ds
where dsterm in ('Early EOT')
),

sv_previous_cycle as (
select
rsv.studyid ,
rsv.siteid,
rsv.usubjid,
rsv.visit_start_dtc as "Previous Cycle Visit Date",
rsv.visit as "Previous Cycle Visit",
rsv.visitnum as visitnum
from cqs.rpt_subject_visit_schedule rsv
where (studyid ,siteid,usubjid,visitnum)
in( select studyid,siteid,usubjid, max(lead_visitnum) from(
select studyid,siteid,usubjid,visitnum,visit_start_dtc,lead(visitnum,1,null) over(partition by studyid,siteid,usubjid order by visit_start_dtc desc) as lead_visitnum 
from cqs.rpt_subject_visit_schedule
where visit_start_dtc is not null
and expected = 'true'
) b group by 1,2,3
)
),

sv_data_previous as --this is used when previous visit is coming null from rpt_subject_visit_schedule
(
select studyid, siteid, usubjid, visit,svstdtc, visitnum
from cqs.sv 
where (studyid, siteid, usubjid, svstdtc, visitnum) in
(select studyid, siteid, usubjid, svstdtc, min(visitnum) 
from cqs.sv
where (studyid, siteid, usubjid, svstdtc) in
(select studyid, siteid, usubjid , max(lead_svstdtc)
from (select studyid,siteid,usubjid,visitnum,svstdtc,lead(svstdtc,1,null) 
over(partition by studyid,siteid,usubjid order by svstdtc desc,visitnum desc) as lead_svstdtc
from cqs.sv
where visitnum != 99
)a
group by 1,2,3)
and visitnum != 99
group by 1,2,3,4
)
),


sv_data_current as --this is used when current visit is coming null from rpt_subject_visit_schedule
(
select studyid, siteid, usubjid, visit, svstdtc, visitnum
from cqs.sv
where ((studyid, siteid, usubjid, svstdtc,visitnum) in
(select studyid, siteid, usubjid, svstdtc, max(visitnum) from cqs.sv
where (studyid, siteid, usubjid, svstdtc) in
(select studyid, siteid, usubjid,  max(svstdtc) 
from cqs.sv 
where visitnum != 99
group by 1,2,3)
and visitnum != 99
group by 1,2,3,4
))),

sv_current_cycle as 
(
select
rsv.studyid ,
rsv.siteid,
rsv.usubjid,
rsv.visit_start_dtc as "Current Cycle Visit Date",
rsv.visit as "Current Cycle Visit",
rsv.visitnum
from cqs.rpt_subject_visit_schedule rsv
where (rsv.studyid ,rsv.siteid,rsv.usubjid,rsv.visitnum,rsv.visit_start_dtc)
in(
select studyid,siteid,usubjid,max(visitnum) as visitnum,max(visit_start_dtc) as visit_start_dtc from cqs.rpt_subject_visit_schedule
where visit_start_dtc is not null
and 
expected = 'true'
group by 1,2,3
)
),

sv_next_cycle as
(
select
studyid ,
siteid,
usubjid,
expectedvisitdate as "Next Expected Cycle Visit Date",
visit as "Next Expected Cycle Visit",
visitnum as visitnum
from cqs.rpt_subject_visit_schedule
where (studyid ,siteid,usubjid,visitnum)
in(
select studyid,siteid,usubjid,min(visitnum) as visitnum from cqs.rpt_subject_visit_schedule
where visit_start_dtc is null
and expected = 'false'
group by 1,2,3
)
),


 pages_entered as
(
select 	"Study" as studyid, "Site" as siteid, "Subject" as usubjid, sum( "Pages Completed") as "Pages Entered" 
from    ckpi."ckpi_missing_stream_pages"
group by 1,2,3
),

pages_expected as
(
select 		"Study" as studyid, "Site" as siteid, "Subject" as usubjid, sum( "Pages Expected") as "Pages Expected" 
from    ckpi."ckpi_missing_stream_pages"
group by 1,2,3
),

query_marking_group as
(
with query_data_open as
(
select studyid,
siteid,
usubjid,
querytype,
count(querystatus) as "Queries Open"
from cqs.query
where querystatus = 'Open'
group by 1,2,3,4
),

query_data_answered as
(
select studyid,
siteid,
usubjid,
querytype,
count(querystatus) as "Queries Answered"
from cqs.query
where querystatus = 'Answered'
group by 1,2,3,4
)

select distinct q.studyid,
q.siteid,
q.usubjid,
q.querytype as "Marking Group",
--count(q.queryid) as "Queries by Marking Group",
"Queries Open",
"Queries Answered"
from cqs.query q
left join query_data_open qo
on
qo.studyid = q.studyid and
qo.siteid = q.siteid and
qo.usubjid = q.usubjid and
qo.querytype = q.querytype
left join query_data_answered qa
on
qa.studyid = q.studyid and
qa.siteid = q.siteid and
qa.usubjid = q.usubjid and
qa.querytype = q.querytype
)/*,

 coding_complete as
(
select
'TAS120_201' as studyid,
'TAS120_201_'||split_part("sitename",'_',1) as siteid,
"subjectname" as usubjid,
case when sum(requirescoding::int)= 0 then 'Yes'
else 'No' end as "Coding Complete"
from tas120_201.stream_page_status
group by 1,2,3

union

select
'TAS120_202' as studyid,
'TAS120_202_'||split_part("sitename",'_',1) as siteid,
"subjectname" as usubjid,
case when sum(requirescoding::int)= 0 then 'Yes'
else 'No' end as "Coding Complete"
from tas120_202.stream_page_status
group by 1,2,3

union

select
'TAS0612_101' as studyid,
'TAS0612_101_'||split_part("sitename",'_',1) as siteid,
"subjectname" as usubjid,
case when sum(requirescoding::int)= 0 then 'Yes'
else 'No' end as "Coding Complete"
from TAS0612_101.stream_page_status
group by 1,2,3

union

select
'TAS3681_101_DOSE_ESC' as studyid,
trim('TAS3681101_'||split_part("sitename",'-',1)) as siteid,
"subjectname" as usubjid,
case when sum(requirescoding::int)= 0 then 'Yes'
else 'No' end as "Coding Complete"
from TAS3681_101.stream_page_status
where "subjectname" in (select distinct usubjid from cqs.subject where studyid = 'TAS3681_101_DOSE_ESC')
group by 1,2,3

union

select
'TAS3681_101_DOSE_EXP' as studyid,
trim('TAS3681101_'||split_part("sitename",'-',1)) as siteid,
"subjectname" as usubjid,
case when sum(requirescoding::int)= 0 then 'Yes'
else 'No' end as "Coding Complete"
from TAS3681_101.stream_page_status
where "subjectname" in (select distinct usubjid from cqs.subject where studyid = 'TAS3681_101_DOSE_EXP')
group by 1,2,3

union

select
'TAS120_203' as studyid,
'TAS120_203_'||split_part("sitename",'_',1) as siteid,
"subjectname" as usubjid,
case when sum(requirescoding::int)= 0 then 'Yes'
else 'No' end as "Coding Complete"
from tas120_203.stream_page_status
group by 1,2,3

union

select
'TAS120_204' as studyid,
'TAS120_204_'||split_part("sitename",'_',1) as siteid,
"subjectname" as usubjid,
case when sum(requirescoding::int)= 0 then 'Yes'
else 'No' end as "Coding Complete"
from tas120_204.stream_page_status
group by 1,2,3

union

select
'TAS117_201' as studyid,
'TAS117_201_'||split_part("sitename",'_',1) as siteid,
"subjectname" as usubjid,
case when sum(requirescoding::int)= 0 then 'Yes'
else 'No' end as "Coding Complete"
from tas117_201.stream_page_status
group by 1,2,3
union

select
'TAS2940_101' as studyid,
'TAS2940_101_'||split_part("sitename",'_',1) as siteid,
"subjectname" as usubjid,
case when sum(requirescoding::int)= 0 then 'Yes'
else 'No' end as "Coding Complete"
from TAS2940_101.stream_page_status
group by 1,2,3
),

edc_clean as
(
select distinct q.studyid,
q.siteid,
q.usubjid,
'Yes' as "EDC Clean"
from cqs.query q
join coding_complete cc
on q.studyid = cc.studyid and
q.siteid = cc.siteid and
q.usubjid = cc.usubjid
where cc."Coding Complete" = 'Yes'
and (q.studyid ,q.usubjid) not in (
select studyid,usubjid from cqs.query where querystatus = 'Open'
)

union

select distinct q.studyid,
q.siteid,
q.usubjid,
null as "EDC Clean"
from cqs.query q
join coding_complete cc
on q.studyid = cc.studyid and
q.siteid = cc.siteid and
q.usubjid = cc.usubjid
where cc."Coding Complete" = 'Yes'
and q.querystatus = 'Open'
)*/
,
sdv_n as
(
select
f1.studyid,
siteid,
usubjid,
count(*)::decimal  as numerator
from cqs.fielddata f1
left join cqs.fielddef f2
on f1.studyid = f2.studyid and
f1.formid = f2.formid and
f1.fieldid = f2.fieldid
where f2.issdv = true
and sdvdate is not null
group by 1,2,3

),

sdv_d as
(
select
f1.studyid,
siteid,
usubjid,
count(*)::decimal as denominator
from cqs.fielddata f1
left join cqs.fielddef f2
on f1.studyid = f2.studyid and
f1.formid = f2.formid and
f1.fieldid = f2.fieldid
where f2.issdv = true
group by 1,2,3
),

subject_sdv as
(
select n.*, d.denominator ,
round(((numerator * 100)/denominator),2) as "Subject SDV"
from sdv_n n
join sdv_d d
on n.studyid = d.studyid and
n.siteid = d.siteid and
n.usubjid = d.usubjid
),

survival_status as (
select * from (
select
"project" as studyid,
"SiteNumber" as siteid,
"Subject" as usubjid,
"SFCAT" as "Survival Status",
("SFCNFDAT") as "Survival Status Date",
rank()over(partition by "project","SiteNumber","Subject" order by "SFCNFDAT" desc) as "Rank"
from tas120_201."SF"
)a where a."Rank"=1 and usubjid not in (select "Subject" from tas120_201."DTH" d)

union

select * from (
select
"project" as studyid,
"SiteNumber" as siteid,
"Subject" as usubjid,
"SFCAT" as "Survival Status",
("SFCNFDAT") as "Survival Status Date",
rank()over(partition by "project","SiteNumber","Subject" order by "SFCNFDAT" desc) as "Rank"
from tas120_202."SF"
)a where a."Rank"=1 and usubjid not in (select "Subject" from tas120_202."DTH" d)

union

select * from (
select
"project" as studyid,
'TAS0612_101_'||split_part("SiteNumber",'_',2) as siteid,
"Subject" as usubjid,
"SFCAT" as "Survival Status",
("SFCNFDAT") as "Survival Status Date",
rank()over(partition by "project","SiteNumber","Subject" order by "SFCNFDAT" desc) as "Rank"
from TAS0612_101."SF"
)a where a."Rank"=1 and usubjid not in (select "Subject" from tas0612_101."DTH" d)


union

select * from (
select
'TAS3681_101_DOSE_ESC' as studyid,
"SiteNumber" as siteid,
"Subject" as usubjid,
"SFCAT" as "Survival Status",
("SFCNFDAT") as "Survival Status Date",
rank()over(partition by "project","SiteNumber","Subject" order by "SFCNFDAT" desc) as "Rank"
from TAS3681_101."SF"
)a where a."Rank"=1
and "usubjid" in (select distinct usubjid from cqs.subject where studyid = 'TAS3681_101_DOSE_ESC')
and usubjid not in (select "Subject" from tas3681_101."DTH" d)

union

select * from (
select
'TAS3681_101_DOSE_EXP' as studyid,
"SiteNumber" as siteid,
"Subject" as usubjid,
"SFCAT" as "Survival Status",
("SFCNFDAT") as "Survival Status Date",
rank()over(partition by "project","SiteNumber","Subject" order by "SFCNFDAT" desc) as "Rank"
from TAS3681_101."SF"
)a where a."Rank"=1
and "usubjid" in (select distinct usubjid from cqs.subject where studyid = 'TAS3681_101_DOSE_EXP')
and usubjid not in (select "Subject" from tas3681_101."DTH" d)

union

select * from (
select
"project" as studyid,
'TAS120_203_'||split_part("SiteNumber",'_',2) as siteid,
"Subject" as usubjid,
"SFCAT" as "Survival Status",
("SFCNFDAT") as "Survival Status Date",
rank()over(partition by "project","SiteNumber","Subject" order by "SFCNFDAT" desc) as "Rank"
from tas120_203."SF"
)a where a."Rank"=1 and usubjid not in (select "Subject" from tas120_203."DTH" d)

/*union Table doesn't exit in DB

select * from (
select
"project" as studyid,
'TAS120_204_'||split_part("SiteNumber",'_',2) as siteid,
"Subject" as usubjid,
"SFCAT" as "Survival Status",
("SFCNFDAT") as "Survival Status Date",
rank()over(partition by "project","SiteNumber","Subject" order by "SFCNFDAT" desc) as "Rank"
from tas120_204."SF"
)a where a."Rank"=1*/

union

select * from (
select
"project" as studyid,
'TAS117_201_'||split_part("SiteNumber",'_',2) as siteid,
"Subject" as usubjid,
"SFCAT" as "Survival Status",
("SFCNFDAT") as "Survival Status Date",
rank()over(partition by "project","SiteNumber","Subject" order by "SFCNFDAT" desc) as "Rank"
from TAS117_201."SF"
)a where a."Rank"=1 and usubjid not in (select "Subject" from tas117_201."DTH" d)

/*union Table doesn't exit in DB

select * from (
select
"project" as studyid,
'TAS2940_101_'||split_part("SiteNumber",'_',2) as siteid,
"Subject" as usubjid,
"SFCAT" as "Survival Status",
("SFCNFDAT") as "Survival Status Date",
rank()over(partition by "project","SiteNumber","Subject" order by "SFCNFDAT" desc) as "Rank"
from tas2940_101."SF"
)a where a."Rank"=1*/

)



select distinct
su.studyid as Study,
dm.arm as Cohort,
su.siteid as Site,
su.usubjid as Subject,
"Randomization or Screen Fail Date",
su.status as "Subject Status",
"EOT Date",
--coalesce("Date of Death","EOS Date") as "EOS Date",
"EOS Date" as "EOS Date",
"Date of Death",
coalesce(svpc."Previous Cycle Visit Date", sdp.svstdtc) as "Previous Cycle Visit Date",
coalesce(svpc."Previous Cycle Visit",sdp.visit) as "Previous Cycle Visit",
coalesce("Current Cycle Visit Date", sdc.svstdtc, sv.svstdtc) as "Current Cycle Visit Date",
coalesce("Current Cycle Visit", sdc.visit, sv.visit) as "Current Cycle Visit",
case when (su.status not in ('Completed','Failed Screen','Failed Randomization','Withdrawn') and "Date of Death" is null ) then "Next Expected Cycle Visit Date"
else null end "Next Expected Cycle Visit Date",
case when (su.status not in ('Completed','Failed Screen','Failed Randomization','Withdrawn') and "Date of Death" is null ) then "Next Expected Cycle Visit"
else null end "Next Expected Cycle Visit",
"Pages Entered",
"Pages Expected",
"Queries Open",
"Queries Answered",
--"Coding Complete",
--case when (("Pages Entered" = "Pages Expected") and  "EDC Clean" = 'Yes') then 'Yes' end as "EDC Clean" ,
"Subject SDV",
case when "Subject SDV"= 100 then 'Yes' end as "CRO Clean",
"Marking Group"
, "Survival Status"
,"Survival Status Date"
from
cqs.subject su
left join cqs.dm on su.studyid = dm.studyid and su.siteid = dm.siteid and su.usubjid = dm.usubjid
left join subject_status ss on su.studyid = ss.studyid and su.siteid = ss.siteid and su.usubjid = ss.usubjid
left join ds_data_enrolled ds1 on ds1.studyid = dm.studyid and ds1.siteid = dm.siteid and ds1.usubjid = dm.usubjid
left join ds_data_completed ds2 on ds2.studyid = dm.studyid and ds2.siteid = dm.siteid and ds2.usubjid = dm.usubjid
left join sv_eot_date sv1 on sv1.studyid = su.studyid and sv1.siteid = su.siteid and sv1.usubjid = su.usubjid
left join DTH_data dth on dth.studyid = su.studyid and dth.siteid = su.siteid and dth.usubjid = su.usubjid
left join sv_current_cycle svcc on su.studyid = svcc.studyid and su.siteid = svcc.siteid and su.usubjid = svcc.usubjid
left join sv_next_cycle svnc on svnc.studyid = su.studyid and svnc.siteid = su.siteid and svnc.usubjid = su.usubjid
left join pages_entered pe on su.studyid = pe.studyid and su.siteid = pe.siteid and su.usubjid = pe.usubjid
left join pages_expected pp on pp.studyid = su.studyid and pp.siteid = su.siteid and pp.usubjid = su.usubjid
--left join coding_complete cc on su.studyid = cc.studyid and su.siteid = cc.siteid and su.usubjid = cc.usubjid
--left join edc_clean ec on cc.studyid = ec.studyid and cc.siteid = ec.siteid and cc.usubjid = ec.usubjid
--left join subject_sdv sdv  on ec.studyid = sdv.studyid and ec.siteid = sdv.siteid and ec.usubjid = sdv.usubjid
left join subject_sdv sdv  on su.studyid = sdv.studyid and su.siteid = sdv.siteid and su.usubjid = sdv.usubjid
left join query_marking_group mg on su.studyid = mg.studyid and su.siteid = mg.siteid and su.usubjid = mg.usubjid
left join sv_previous_cycle svpc on su.studyid = svpc.studyid and su.siteid = svpc.siteid and su.usubjid = svpc.usubjid
left join survival_status sst on su.studyid = sst.studyid and su.siteid = sst.siteid and su.usubjid = sst.usubjid
left join  sv_data_current sdc on su.studyid = sdc.studyid and su.siteid = sdc.siteid and su.usubjid = sdc.usubjid 
left join  sv_data_previous sdp on su.studyid = sdp.studyid and su.siteid = sdp.siteid and su.usubjid = sdp.usubjid
left join cqs.sv sv on su.studyid = sv.studyid and su.siteid = sv.siteid and su.usubjid = sv.usubjid and sv.svstdtc is null 
and sv.visitnum = 1
