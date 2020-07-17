--The below code generates a path to conversion per cookie, including search and display activity and identifying whether that cookie is an existing customer or not
--The next step is to aggregate and count frequency by path to understand what are the commonly taken paths to on-site conversion 

------------------------------------
##SEARCH AND DISPLAY AND ACTIVITY##
------------------------------------

#standardsql
WITH subq1 as
(SELECT
	User_ID,
	CONCAT('[S]', s.Paid_Search_Ad_Group) AS Activity,
	c.Event_Time
FROM
	`prod_dcm.dcm_click` c
LEFT JOIN `prod_dcm.dcm_match_table_paid_search` s
ON c.Segment_Value_1 = s.Paid_Search_Legacy_Keyword_ID
WHERE
	c.Advertiser_ID = "6269322" --change your advertiser ID
	AND c.campaign_ID in ("20372764") --include your campaign IDs separated by commas
	AND User_ID!='0'
	and Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-30'

UNION ALL

SELECT
	User_ID,
	CONCAT('[DClk]', IF(c.campaign like '%CIT1%', 'ATL', IF(c.campaign like '%CIT2%', 'BTL', 'Other'))
		, '|',SPLIT(p.placement, '_')[SAFE_OFFSET(6)] --product
		, '|', SPLIT(p.placement, '_')[SAFE_OFFSET(7)] --site
		) AS Activity,
	i.Event_Time
	FROM `prod_dcm.dcm_click` i
	left join `prod_dcm.dcm_match_table_campaigns` c
	ON i.campaign_id = c.campaign_id
  left join `prod_dcm.dcm_match_table_placements` p
  ON i.placement_ID = p.placement_ID
WHERE
	i.Advertiser_ID = "6269322" --change your advertiser ID
	AND i.campaign_ID != "20372764" --not SEM campaign
	AND User_ID!='0'
	and Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-30'

UNION ALL

SELECT
	User_ID,
	CONCAT('[DImp]', IF(c.campaign like '%CIT1%', 'ATL', IF(c.campaign like '%CIT2%', 'BTL', 'Other'))
		, '|',SPLIT(p.placement, '_')[SAFE_OFFSET(6)] --product
		, '|', SPLIT(p.placement, '_')[SAFE_OFFSET(7)] --site
		) AS Activity,
	i.Event_Time
	FROM `prod_dcm.dcm_impression` i
	left join `prod_dcm.dcm_match_table_campaigns` c
	ON i.campaign_id = c.campaign_id
	left join `prod_dcm.dcm_match_table_placements` p
	ON i.PLACEMENT_ID = p.PLACEMENT_ID
WHERE
	i.Advertiser_ID = "6269322" --change your advertiser ID
	AND i.campaign_ID != "20372764" --not SEM campaign
	AND User_ID!='0'
	and Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-30'

UNION ALL

	SELECT
		User_ID,
		CONCAT('[A]', Activity, ' | ', IFNULL(u4, '')) AS Activity,
		Event_Time
	FROM
		`prod_dcm.dcm_activity` act
	LEFT JOIN `prod_dcm.dcm_match_table_activity_cats` cat
	ON act.Activity_ID = cat.Activity_ID
	WHERE user_id!='0'
	and Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-30'
	and act.activity_id in ("7557639","7591180","7555200","7591183","7567072","7567072","7496874","7567072","7496874",
		"4611609","6644848","4609008","6637343","4607607","6165296","6637109","6637109","6733079","4590710","6644740","4559924","6156211","6645049","6733198","6871405","4625026","6648124","4625025","6641858","4627801","6642677","4611735","6648484","4625020","6635363","6730948","6732145","6730951","6728830","6730954",
		"7189060","7196423","7196426","7196417","7196420","7197314","7280597","7280600","7189060","7196423","7196426","7280597","7280597","7280600","7496874","7516088","7496292","7496880","7496877","7496589","7496883","7514903",
		"4609009","4613001","4611610","4612407","4608410","4611607","6178413","6174529","6173983","4610810","4611608","4609609","6737350","6734036","6733082","4611411","4607008","4609007","6635873","6644743","6637100","4589949","4590712","4590713","6155659","6156555","6149669","6640661","6645985","6640658","6733201","6733204","6729016"
	) -- all tags used here (conversion tags, landing pages, app starts and post login)
	and user_id in
		(SELECT User_ID
		 FROM `prod_dcm.dcm_click`
		 WHERE
		 Advertiser_ID = "6269322" --change your advertiser ID
		 AND User_ID!='0'
		 and Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-30'

		 UNION ALL

		 SELECT User_ID
 		 FROM `prod_dcm.dcm_impression`
 		 WHERE
 		 Advertiser_ID = "6269322" --change your advertiser ID
 		 AND User_ID!='0'
 		and Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-30'
	 )
)

SELECT *, CASE WHEN User_ID IN
          (
            SELECT USER_ID
            FROM `prod_dcm.dcm_activity`
            WHERE ACTIVITY_ID IN ("6730954","6728830","6730951","6732145","6730948","6871405") -- enter the ECM (post-login page) Activity IDs in here
            AND Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-31'
            AND User_ID!='0'
            AND USER_ID not in (
            	SELECT User_id
	        	FROM prod_dcm.dcm_activity
	        	WHERE activity_id IN ("6733201","6640661","6155659","4589949","6635873","4611411","6737350","4610810","6178413","4612407","4609009"
						) --approval page tags
	            AND USER_ID != "0"
	            and Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-30'
            	)

            UNION ALL

            (SELECT login.User_ID
	   			FROM
	   			(SELECT User_ID, Event_Time login_time
		            FROM prod_dcm.dcm_activity
		            WHERE User_ID != "0"
		            AND ACTIVITY_ID IN ("6730954","6728830","6730951","6732145","6730948","6871405") --postlogin
		            and Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-30'
	            ) login

	  			INNER JOIN

	  			(SELECT User_id, Event_Time appr_time
		           	FROM prod_dcm.dcm_activity
		            WHERE activity_id IN ("6733201","6640661","6155659","4589949","6635873","4611411","6737350","4610810","6178413","4612407","4609009"
								) --approval
		            AND USER_ID != "0"
		            and Event_Date >= '2018-07-01'AND Event_Date <= '2018-07-30'
	            ) appr

	    		ON login.User_ID = appr.User_ID
	     		WHERE ((UNIX_SECONDS(timestamp(login_time)) - unix_seconds(timestamp(appr_time)))/60) < 0 )

            ) THEN 1 ELSE 0 END AS ECM,

CASE WHEN User_ID IN
          (
            SELECT USER_ID
            FROM `prod_dcm.dcm_activity`
            WHERE ACTIVITY_ID IN ("4609009","4613001","4611610","4612407","4608410","4611607","6178413","6174529","6173983","4610810","4611608","4609609","6737350","6734036","6733082","4611411","4607008","4609007","6635873","6644743","6637100","4589949","4590712","4590713","6155659","6156555","6149669","6640661","6645985","6640658","6733201","6733204","6729016"
																	) -- enter the Converter Activity IDs in here
            and Event_Date >= '2018-07-01'AND Event_Date <= '2018-08-14'
            AND User_ID!='0') THEN 1 ELSE 0 END AS Convertors
from subq1
