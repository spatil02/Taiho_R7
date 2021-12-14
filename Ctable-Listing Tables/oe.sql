/*Mapping script for Ophthalmological Examination - Listing
Table name : "ctable_listing"."cTable_oe"
Project name : Taiho*/


CREATE SCHEMA IF NOT EXISTS "ctable_listing";

drop table if exists "ctable_listing"."cTable_oe";

create table "ctable_listing"."cTable_oe" as
with oe as	(select
"project" "Project", 
"SiteNumber",
"Site",
"Subject" "Subject",
"RecordId",
"OEPERF" ,
"FolderName" ,
"OEDAT"::date  ,
"OEABN" ,
case when "OEABN" = 'Yes' then "OEL_DESC" when "OEABN" = 'No' then "RSLBL_DESC" else
"OEL_DESC" end as  "RSLBR_DESC",
case when "OEABN" = 'Yes' then "OER_DESC" when "OEABN" = 'No' then "RSLBR_DESC" else
"OER_DESC" end as "SLBL_DESC"
from
tas120_201."OPE"
union all
select
"project" "Project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
null "OEPERF",
"FolderName" ,
"OPEDAT"::date "OEDAT" ,
null as "OEABN",
coalesce("OPELTABN", "OPELTABN1") as "RSLBR_DESC",
coalesce("OPERTABN", "OPERTABN1") as "SLBL_DESC"
from tas120_202."OPE"
union all 
select 
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) as "SiteNumber",
"Site",
"Subject",
"RecordId",
null "OEPERF",
"FolderName" ,
"OPEDAT"::date "OEDAT" ,
null as "OEABN",
coalesce("OPELEABN","OPELECLSP") as "RSLBR_DESC",
coalesce("OPEREABN", "OPERECLSP") as "SLBL_DESC" 
from tas120_203."OPE" o 
union all 
select 
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) as "SiteNumber",
"Site",
"Subject",
"RecordId",
null "OEPERF",
"FolderName" ,
"OEDAT"::date "OEDAT" ,
"OEABN" as "OEABN",
case when "OEABN" = 'Yes' then "OEL_DESC" when "OEABN" = 'No' then "RSLBL_DESC" else
"OEL_DESC" end as  "RSLBR_DESC",
case when "OEABN" = 'Yes' then "OER_DESC" when "OEABN" = 'No' then "RSLBR_DESC" else
"OER_DESC" end as "SLBL_DESC" 
from tas120_204."OPE" o
union all 
select 
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) as "SiteNumber",
"Site",
"Subject",
"RecordId",
null "OEPERF",
"FolderName" ,
"OEDAT"::date "OEDAT" ,
null as "OEABN",
null as "RSLBR_DESC",
null as "SLBL_DESC" 
from tas2940_101."OPE" o  
) 

select	oe."Project",
		oe."SiteNumber",
		oe."Site",
		oe."Subject",
		oe."RecordId",
		oe."OEPERF",
		oe."FolderName",
		oe."OEDAT" ,
		oe."OEABN",
		oe."RSLBR_DESC",
		oe."SLBL_DESC",
		(oe."Project"||'~'||oe."SiteNumber"||'~'||oe."Site"||'~'||oe."Subject"||'~'||oe."FolderName"||'~'||oe."RecordId") as objectuniquekey
from 	oe;

	
--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-stage-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-app-clinical-master-write";	

