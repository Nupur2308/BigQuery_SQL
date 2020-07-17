--The below code creates a reach-convertor overlap table which identifies the reach overlap and converter overlap by partner and tactic within an advertiser 

CREATE TEMP FUNCTION SUM_Impressions(a FLOAT64,
    b FLOAT64 )
  RETURNS FLOAT64
  LANGUAGE js AS ''' return a+b; '''; WITH subQ1 AS (
  SELECT
    User_ID,
    a.Advertiser_ID,
    a.Campaign_ID,
    Ad_ID,
    a.Event_Time AS imps_time,
    a.Placement_ID,
    c.Campaign,
    b.Placement,
    SPLIT( b.Placement, '_')[SAFE_OFFSET(2)] AS Tactic,
    SPLIT(b.Placement, '_')[SAFE_OFFSET(7)] AS Partner
  FROM (`citi-analytics.prod_dcm.dcm_impression` AS a
    LEFT JOIN
      `citi-analytics.prod_dcm.dcm_match_table_placements` AS b
    ON
      a.Placement_ID= b.Placement_ID )
  LEFT JOIN
    `citi-analytics.prod_dcm.dcm_match_table_campaigns` AS c
  ON
    a.Campaign_ID=c.Campaign_ID
  WHERE
    a.User_ID <> '0'
    AND a.Advertiser_ID IN( '6269322')
    AND a.Campaign_ID IN('22045360')
    AND SPLIT(b.placement, '_')[SAFE_OFFSET(1)] = "PRO"
    --and SPLIT(b.Placement, '_')[SAFE_OFFSET(7)] != "SPFB"
    AND a.Event_Date >= '2019-01-01'AND a.Event_Date<='2019-02-25'),
  subQ2 AS (
  SELECT
    COUNT(User_ID) AS Impressions,
    User_ID,
    Campaign_ID,
    Campaign,
    Tactic,
    Partner
  FROM
    subQ1
  WHERE
    Partner<>"NULL"
  GROUP BY
    User_ID,
    Tactic,
    Partner,
    Campaign_ID,
    Campaign),

  conv AS (
  SELECT
    User_ID,
    COUNT( DISTINCT User_ID) AS Conversions
  FROM (
    SELECT
      a.User_ID,
      Activity_ID,
      Event_Time AS conv_date,
      subQ1.imps_time,
      Event_Date
    FROM
      `citi-analytics.prod_dcm.dcm_activity` AS a
    LEFT JOIN
      subQ1 #try inner join
    ON
      a.User_ID=subQ1.User_ID
    WHERE
      Event_date>= '2019-01-01'AND Event_date<='2019-03-11'
      AND Activity_ID IN("4625429","4609009","4611610","4627803","6648466","6635954","6644851","6648121","4612407","4611607","4625237","4625428","6642140","6640733","6635960","6622740","4634807","4608819","4609013","4635406","6178413","6173983","4625022","4610810","4609609","4625426","6622743","6637106","6645988","6642680","4635603","4610812","4611415","4635697","4702270","4700305","6737350","6733082","4635805","4634806","4611411","4609007","4635408","4635009","6622746","6635873","6637100","6618156","4590711","4609616","4625232","4624726","4589949","4590713","4699528","6155659","6149669","4700520","6622752","6640661","6640658","6831111","6733201","6729016","6839247","6622755","4608497","4625414","4611420","4610816")
      AND ((UNIX_SECONDS(TIMESTAMP(Event_Time)) - unix_seconds(TIMESTAMP(subQ1.imps_time)))) <= 14*24*60*60
      AND ((UNIX_SECONDS(TIMESTAMP(Event_Time)) - unix_seconds(TIMESTAMP(subQ1.imps_time))))>=0 )
  GROUP BY
    User_ID),



 overall_conv as (
SELECT
  Campaign_ID,
  Campaign,
  Partner,
  Tactic,
  COUNT(Conversions) as Overall_Convertor
FROM ( select subQ1.User_ID,
    subQ1.Campaign_ID,
    subQ1.Campaign,
    subQ1.Partner,
    subQ1.Tactic,conv.Conversions
  FROM
    subQ1
  LEFT JOIN
    conv
  ON
    subQ1.User_ID=conv.User_ID)
GROUP BY
  Campaign_ID,
  Campaign,
  Partner,
  Tactic),


 subQ3 AS (
  SELECT
    a.impressions AS a_impressions,
    b.impressions AS b_impressions,
    SUM_Impressions(a.Impressions,
      b.Impressions) AS Impression_Overlap,
    a.User_ID,
    a.Campaign_ID AS A_Campaign_ID,
    b.Campaign_ID AS B_Campaign_ID,
    a.Campaign AS a_Campaign,
    b.Campaign AS b_Campaign,
    a.Partner AS a_Partner,
    b.Partner AS b_Partner,
    a.tactic AS a_Tactic,
    b.tactic AS b_Tactic
  FROM
    subQ2 AS a
  INNER JOIN
    subQ2 AS b
  ON
    a.User_ID=b.User_ID
    ),


imps_conv as (
SELECT
subQ3.a_impressions,subQ3.b_impressions,subQ3.Impression_Overlap,
subQ3.User_ID, subQ3.A_Campaign_ID, subQ3.B_Campaign_ID, subQ3.a_Campaign, subQ3.b_Campaign, subQ3.a_Partner, subQ3.b_Partner, subQ3.a_Tactic, subQ3.b_Tactic,
conv.Conversions

from subQ3 left join conv on subQ3.User_ID=conv.User_ID),




  SubQ4 AS(
  SELECT
    COUNT(DISTINCT User_ID) AS Reach_Overlap,
    SUM(a_impressions) AS SUM_a_Impressions,
    SUM(b_impressions) AS SUM_b_Impressions,
    SUM(Impression_Overlap) AS SUM_Impression_Overlap,
    SUM(Impression_Overlap)/ COUNT(DISTINCT User_ID) AS Overlap_Frequency,
    SUM(Conversions) as Overlap_Conversions,
    A_Campaign_ID,
    B_Campaign_ID,
    a_Campaign,
    b_Campaign,
    a_Partner,
    b_Partner,
    a_Tactic,
    b_Tactic
  FROM
    imps_conv
  GROUP BY
    A_Campaign_ID,
    B_Campaign_ID,
    a_Partner,
    b_Partner,
    a_Tactic,
    b_Tactic,
    a_Campaign,
    b_Campaign),
  subQ5 AS (
  SELECT
    COUNT(DISTINCT User_ID) AS Unique_Reach,
    SUM(Impressions)/ COUNT(DISTINCT User_ID) AS Frequency,
    SUM(Impressions) AS Total_Impressions,
    a.Campaign_ID,
    a.Campaign,
    a.Partner,
    a.Tactic,
    AVG(Overall_Convertor) as Overall_Convertor

  FROM
    subQ2 as a

    left join

    overall_conv as  b

    on   a.Campaign_ID=b.Campaign_ID and a.Partner=b.Partner and a.Tactic=b.Tactic
  GROUP BY
     a.Campaign_ID,
    a.Campaign,
    a.Partner,
    a.Tactic)
SELECT
  Reach_Overlap,
  SUM_Impression_Overlap,
  SUM_a_Impressions,
  SUM_b_Impressions,
  Overlap_Frequency,
  Overlap_Conversions,
  A_Campaign_ID,
  B_Campaign_ID,
  a_Campaign,
  b_Campaign,
  a_Partner,
  b_Partner,
  a_Tactic,
  b_Tactic,
  Unique_Reach,
  Frequency,
  Total_Impressions,
  Overall_Convertor
FROM
  subQ4 AS a
LEFT JOIN
  subQ5 AS b
ON
  A_Campaign_ID= b.Campaign_ID
  AND a.a_Partner=b.Partner
  AND a.a_Tactic=b.Tactic
