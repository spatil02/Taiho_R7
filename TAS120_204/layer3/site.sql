/*
CCDM Site mapping
Notes: Standard mapping to CCDM Site table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

    site_data AS (
                SELECT  distinct 'TAS120_204'::text AS studyid,
                        'TAS120_204'::text AS studyname,
                        concat(concat('TAS120_204','_'),substring("name",1,3))::text AS siteid,
                        substring(trim("name"),5)::text AS sitename,
                        'Syneos'::text AS croid,
                        'Syneos'::text AS sitecro,
                        'United States of America'::text AS sitecountry,
                        null::text AS sitecountrycode,
                        'North America'::text AS siteregion,
                        'TRUE'::text as statusapplicable,
                        null::date AS sitecreationdate,
                        null::date AS siteactivationdate,
                        null::date AS sitedeactivationdate,
                        null::text AS siteinvestigatorname,
                        null::text AS sitecraname,
                        null::text AS siteaddress1,
                        null::text AS siteaddress2,
                        null::text AS sitecity,
                        null::text AS sitestate,
                        null::text AS sitepostal,
                        null::text AS sitestatus,
                        null::date AS sitestatusdate
                        from tas120_204.__sites),

    sitecountrycode_data AS (
                SELECT studyid, countryname_iso, countrycode3_iso FROM studycountry)

SELECT 
        /*KEY (s.studyid || '~' || s.siteid)::text AS comprehendid, KEY*/
        s.studyid::text AS studyid,
        s.studyname::text AS studyname,
        s.siteid::text AS siteid,
        s.sitename::text AS sitename,
        s.croid::text AS croid,
        s.sitecro::text AS sitecro,
        s.sitecountry::text AS sitecountry,
        cc.countrycode3_iso::text AS sitecountrycode,
        s.siteregion::text AS siteregion,
        s.sitecreationdate::date AS sitecreationdate,
        s.siteactivationdate::date AS siteactivationdate,
        s.sitedeactivationdate::date AS sitedeactivationdate,
        s.siteinvestigatorname::text AS siteinvestigatorname,
        s.sitecraname::text AS sitecraname,
        s.siteaddress1::text AS siteaddress1,
        s.siteaddress2::text AS siteaddress2,
        s.sitecity::text AS sitecity,
        s.sitestate::text AS sitestate,
        s.sitepostal::text AS sitepostal,
        s.sitestatus::text AS sitestatus,
        s.sitestatusdate::date AS sitestatusdate,
        s.statusapplicable::boolean as statusapplicable
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM site_data s 
JOIN included_studies st ON (s.studyid = st.studyid)
LEFT JOIN sitecountrycode_data cc ON (s.studyid = cc.studyid AND LOWER(s.sitecountry)=LOWER(cc.countryname_iso));

