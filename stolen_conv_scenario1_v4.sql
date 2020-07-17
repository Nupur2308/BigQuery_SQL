--Since we operate on a last touch attribution model, conversions get attributed only to the partner that last touched a cookie
--This code identifies whether a particular campaign was not getting the appropriate conversion attribution due overlap with another campaign 

#standardsql

with
subq1 as
(
select
q.user_id, q.interaction_time as interaction_time, q.interaction_type as interaction_type, q.interaction_campaign as interaction_campaign, q.site_id_dcm as interaction_site, q.partner as interaction_partner, q.measurable_imps as measurable_imps, q.viewable_imps as viewable_imps,
a.event_time as conversion_time, a.event_sub_type as conversion_type, camp.campaign as conversion_campaign, a.site_id_dcm as conversion_site, SPLIT(p.Placement, '_')[SAFE_OFFSET(7)] AS conversion_partner from
(
  SELECT i.User_ID, i.event_time as interaction_time , i.event_sub_type as interaction_type, c.campaign as interaction_campaign, i.site_id_dcm, SPLIT(p.Placement, '_')[SAFE_OFFSET(7)] AS partner, active_view_measurable_impressions as measurable_imps, active_view_viewable_impressions as viewable_imps
  FROM `prod_dcm.dcm_impression` i
  left join `prod_dcm.dcm_match_table_campaigns` c
  ON i.campaign_id = c.campaign_id
  left join `prod_dcm.dcm_match_table_placements` p
  ON i.placement_id = p.placement_id
  where i.User_ID != '0'
  and i.Event_Date >= '2018-08-03'AND i.Event_Date<= '2018-08-18'
  and i.advertiser_id = "6260004"
  and User_ID in
    (SELECT User_ID
    FROM `prod_dcm.dcm_activity`
    WHERE User_ID != "0"
    and Event_Date >= '2018-08-03'AND Event_Date<= '2018-08-20'
    and activity_id = "7213479"
    and advertiser_id = "6260004"
    and event_sub_type !=  "null"
    )

  UNION ALL

    SELECT i.User_ID, i.event_time as interaction_time , i.event_sub_type as interaction_type, c.campaign as interaction_campaign, i.site_id_dcm, SPLIT(p.Placement, '_')[SAFE_OFFSET(7)] AS partner, 1  as measurable_imps, 1 as viewable_imps
    FROM `prod_dcm.dcm_click` i
    left join `prod_dcm.dcm_match_table_campaigns` c
    ON i.campaign_id = c.campaign_id
    left join `prod_dcm.dcm_match_table_placements` p
    ON i.placement_id = p.placement_id
    where i.User_ID != '0'
    and i.Event_Date >= '2018-08-03'AND i.Event_Date<= '2018-08-18'
    and i.advertiser_id = "6260004"
    and User_ID in
      (SELECT User_ID
      FROM `prod_dcm.dcm_activity`
      WHERE User_ID != "0"
      and Event_Date >= '2018-08-03'AND Event_Date<= '2018-08-20'
      and activity_id = "7213479"
      and advertiser_id = "6260004"
      and event_sub_type !=  "null"
      )
) q

left join `prod_dcm.dcm_activity` a
on q.user_id = a.user_id
left join `prod_dcm.dcm_match_table_campaigns` camp
ON a.campaign_id = camp.campaign_id
left join `prod_dcm.dcm_match_table_placements` p
ON a.placement_id = p.placement_id
WHERE a.User_ID != "0"
and Event_Date >= '2018-08-03'AND Event_Date<= '2018-08-20'
and a.activity_id = "7213479"
and a.advertiser_id = "6260004"
and a.event_sub_type !=  "null"
),

subq2 as
(select * from subq1
where conversion_campaign like "%ThankYou%"
and interaction_campaign not like "%ThankYou%"
and interaction_campaign not like "%ViS%"
and conversion_time>interaction_time),

subq3 as
(select *, row_number() over (partition by user_id, conversion_time order by interaction_type, interaction_time desc) as attribution, count(conversion_time) over (partition by user_id, conversion_time) as max_interactions, timestamp_diff(timestamp(conversion_time),timestamp(interaction_time),day) as attribution_window
from subq2
),

subq4 as
(select *
from subq3
where attribution_window <=14),

subq5 as
(select *, min(attribution) over (partition by user_id,conversion_time) as last_int
from subq4)

select * from subq5
where attribution = last_int
