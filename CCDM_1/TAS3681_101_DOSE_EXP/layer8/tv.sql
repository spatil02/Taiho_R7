/*
CCDM TV mapping
Notes: Standard mapping to CCDM TV table
*/

WITH included_studies AS (
	SELECT studyid FROM study
),

tv_scheduled AS (

	SELECT visit_number     as      visitnum, visit, visitdy , window_before_visit_day as "visitwindowbefore", window_after_visit_day as "visitwindowafter"
            FROM tv_internal.tv_tracker_list where studyid='TAS3681-101'
	
),

tv_data AS (
			select studyid,
				   visitnum,
				   trim(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(visit,'<WK[0-9]D[0-9]/>\sExpansion','')
								,'<WK[0-9]D[0-9][0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'Expansion','')
								,'\s\([0-9][0-9]\)','')
								,'\s\([0-9]\)','')
								,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
				    ) as	visit,
				   visitdy,
				   visitwindowbefore,
				   visitwindowafter
			from(	   

	select distinct
		'TAS3681_101_DOSE_EXP'::text AS studyid,
		visitnum::numeric AS visitnum,
		visit::text AS visit,
		visitdy::int AS visitdy,
		visitwindowbefore::int AS visitwindowbefore,
		visitwindowafter::int AS visitwindowafter
	FROM tv_scheduled tvs

	UNION ALL

	SELECT
		DISTINCT sv.studyid::text AS studyid,
		99::numeric AS visitnum,
		sv."visit"::text AS visit,
		99999::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM sv 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM tv_scheduled)

	UNION ALL
	SELECT 
		DISTINCT studyid::text AS studyid,
		'99'::numeric AS visitnum,
		visit::text AS visit,
		'99999'::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM formdata 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM sv) 
	AND (studyid, visit) NOT IN (SELECT studyid, visit FROM tv_scheduled)
  
	)r
)

SELECT
	/*KEY tv.studyid::text AS comprehendid, KEY*/
	tv.studyid::text AS studyid,
	tv.visitnum::numeric AS visitnum,
	tv.visit::text AS visit,
	tv.visitdy::int AS visitdy,
	tv.visitwindowbefore::int AS visitwindowbefore,
	tv.visitwindowafter::int AS visitwindowafter,
	null::boolean AS isbaselinevisit,
	'True'::boolean as isvisible
	/*KEY , (tv.studyid || '~' || tv.visit)::text  AS objectuniquekey KEY*/
	/*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM tv_data tv
JOIN included_studies st ON (st.studyid = tv.studyid)
;
