select * from socmed_sc.twitter_tbl;

--monthly kpi's
select
    to_char(time,'YYYY-MM') as year_month,
    sum(impressions) as total_impression_per_month
from socmed_sc.twitter_tbl
group by 1
order by 1 asc;


--monthly engagement rate
select
    to_char(time,'YYYY-MM') as year_month,
    sum(engagement_rate) as total_engagement_rate_per_month
from socmed_sc.twitter_tbl
group by 1
order by 1 asc;

--Get the mom % change of engagement rate
--latest month, previous month 
--mom% change = (latest month-previous month)/previous month

with cte as (
select
    to_char(time,'YYYY-MM') as year_month,
    sum(engagement_rate) as total_engagement_rate_per_month
    lag(sum(engagement_rate),1) over(order by to_char(time,'YYYY-MM')) as prev_month
from socmed_sc.twitter_tbl
group by 1
order by 1 asc
)
select
    percent_change(total_engagement_rate_per_month, prev_month) 
from cte2
where year_month = (select max(year_month) from cte);

--daily engagement rate
select
    extract(dow from time) as day_num,
    to_char(time, 'day') as date_name,
    round(cast(sum(engagement_rate)as numeric),1) as daily
from socmed_sc.twitter_tbl
group by 1,2
order by 1;

--Get the dod % change of engagement rate
--latest day, previous day
--dod% change = (latest day-previous day)/previous day

with cte as (
select
    extract(month from time) as monthly,
    extract(dow from time) as day_num,
    to_char(time, 'day') as date_name,
    round(cast(sum(engagement_rate)as numeric),1) as daily_er
from socmed_sc.twitter_tbl
group by 1,2,3
order by 1
), cte2 as (
select
    monthly,
    day_num, 
    daily_er as latest_day,
    lag(daily_er,1) over(order by day_num) as prev_day
from cte
where monthly = (select max(monthly) from cte)
order by monthly, day_num
)
select
    percent_change(latest_day, prev_day)
from cte2
where day_num = (select max(day_num) from cte);

--hourly engagement_rate
select
    extract(hour from time) as day_num,
    round(cast(sum(engagement_rate)as numeric),1) as daily_er
from socmed_sc.twitter_tbl
group by 1
order by 1;


--Get the hoh % change of engagement rate
--latest hour, previous hour
--dod% change = (latest hour-previous hour)/previous hour

with cte as (
select
    extract(month from time) as monthly,
    extract(dow from time) as day_num,
    extract(hour from time) as hour_num,
    coalesce(cast(sum(engagement_rate)as numeric), 0.01) as hourly_er
from socmed_sc.twitter_tbl
group by 1,2,3
order by 1
)
select
    monthly,
    day_num,
    hour_num, 
    hourly_er as latest_hour,
    coalesce(lag(hourly_er,1) over(order by hour_num), 0.01) as prev_hour
from cte
where monthly = (select max(monthly) from cte)
--order by monthly, day_num, hour_num
-- )
-- select
--  percent_change(latest_hour,prev_hour ) as latest_er_hoh_change
-- from cte2
-- where hour_num = (select max(hour_num) from cte);


-- monthly impressions vs engagements
select
    to_char(time, 'YYYY-MM') as year_month,
    sum(impressions) as total_impressions,
    sum(engagements) as total_engagements
from socmed_sc.twitter_tbl
group by 1
order by 1;

-- weekly impressions vs engagements
select
    to_char(time, 'YYYY-MM') as year_month,
    extract(dow from time) as weekday,
    to_char(time, 'day') as day_name,
    sum(impressions) as total_impressions,
    sum(engagements) as total_engagements
from socmed_sc.twitter_tbl
group by 1,2,3
order by 1,2;

-- weekly impressions vs engagements
select
    to_char(time, 'YYYY-MM') as year_month,
    extract(dow from time) as weekday,
    to_char(time, 'day') as day_name,
    sum(impressions) as total_impressions,
    sum(engagements) as total_engagements
from socmed_sc.twitter_tbl
group by 1,2,3
order by 1,2;

-- hourly impression vs engagements
select
    extract(month from time) as month,
    extract(day from time) as day,
    extract(hour from time) as time,
    sum(impressions) as total_impressions,
    sum(engagements) as total_engagements
from socmed_sc.twitter_tbl
group by 1,2,3
order by 1,2,3;

-- tweet volume per month
select
    extract(month from time) as month,
    count(distinct tweet) as n_tweet
from socmed_sc.twitter_tbl
group by 1
order by 1;

-- to determine the low tweets per day in a month

with cte as (
select
    extract(month from time) as month,
    extract(day from time) as day,
    count(distinct tweet) as n_tweet
from socmed_sc.twitter_tbl
group by 1,2
order by 1,2
)
select
    month, 
    day,
    n_tweet
from cte
where n_tweet = (select min(n_tweet) from cte);

-- to determine hashtags in a tweet

with cte as (
select 
    unnest(string_to_array(tweet, ' ')) as words, 
    impressions,
    engagements,
    retweets,
    replies,
    likes,
    hashtag_clicks
from socmed_sc.twitter_tbl
)
select
    words as hashtags,
    sum(impressions) total_impressions,
    sum(engagements) total_engagements,
    sum(retweets) total_retweets,
    sum(replies) total_replies,
    sum(likes) total_likes,
    sum(hashtag_clicks) total_hashtag
from cte
where words ilike '%#%'
group by 1
order by 2 desc
limit 10;

-- creating functions to compute for % change
create or replace function percent_change(latest_val numeric, prev_val numeric, decimal_places integer default 1)
returns numeric as
'select round(((latest_val - prev_val)/prev_val)*100, decimal_places);'
language sql
immutable --wont change the db
returns null on null input;

--Get the month over month% change of KPI's impressions, engagements, likes, hashtag_clicks, retweets, replies
--latest month, previous month 
--mom% change = (latest month-previous month)/previous month

-- impressions MOM % Change
with cte as (
select
    to_char(time,'YYYY-MM') as year_month,
    sum(impressions) as total_impression_per_month, 
    lag(sum(impressions),1) over(order by to_char(time,'YYYY-MM') asc) as prev_month
from socmed_sc.twitter_tbl
group by 1
order by 1 asc
)
select 
    percent_change(total_impression_per_month, prev_month)
from cte
where year_month = (select max(year_month) from cte);

-- engagements MOM % Change
with cte as (
select
    to_char(time,'YYYY-MM') as year_month,
    sum(engagements) as total_engagements_per_month, 
    lag(sum(engagements),1) over(order by to_char(time,'YYYY-MM') asc) as prev_month
from socmed_sc.twitter_tbl
group by 1
order by 1 asc
)
select 
    percent_change(total_engagements_per_month, prev_month)
from cte
where year_month = (select max(year_month) from cte);

-- likes MOM % Change
with cte as (
select
    to_char(time,'YYYY-MM') as year_month,
    sum(likes) as total_likes_per_month, 
    lag(sum(likes),1) over(order by to_char(time,'YYYY-MM') asc) as prev_month
from socmed_sc.twitter_tbl
group by 1
order by 1 asc
)
select 
    percent_change(total_likes_per_month, prev_month)
from cte
where year_month = (select max(year_month) from cte);


-- hashtag_clicks MOM % Change
with cte as (
select
    to_char(time,'YYYY-MM') as year_month,
    sum(hashtag_clicks) as total_hashtag_clicks_per_month, 
    lag(sum(hashtag_clicks),1) over(order by to_char(time,'YYYY-MM') asc) as prev_month
from socmed_sc.twitter_tbl
group by 1
order by 1 asc
)
select 
    percent_change(total_hashtag_clicks_per_month, prev_month)
from cte
where year_month = (select max(year_month) from cte);


-- retweets MOM % Change
with cte as (
select
    to_char(time,'YYYY-MM') as year_month,
    sum(retweets) as total_retweets_per_month, 
    lag(sum(retweets),1) over(order by to_char(time,'YYYY-MM') asc) as prev_month
from socmed_sc.twitter_tbl
group by 1
order by 1 asc
)
select 
    percent_change(total_retweets_per_month, prev_month)
from cte
where year_month = (select max(year_month) from cte);


-- replies MOM % Change
with cte as (
select
    to_char(time,'YYYY-MM') as year_month,
    sum(replies) as total_replies_per_month, 
    lag(sum(replies),1) over(order by to_char(time,'YYYY-MM') asc) as prev_month
from socmed_sc.twitter_tbl
group by 1
order by 1 asc
)
select 
    percent_change(total_replies_per_month, prev_month)
from cte
where year_month = (select max(year_month) from cte);

-- forecasting using 3 month moving average
with cte as (
select
    to_char(time, 'YYYY-MM') as month,
    sum(engagement_rate) as n_er
from socmed_sc.twitter_tbl
group by 1
order by 1
)
select
    month,
    round(n_er::numeric, 1) as actual_value,
    round(avg(n_er) over(order by month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )::numeric,1) as three_month_moving_avg
from cte
group by month, n_er;

-- forecasting using naive forecasting and checking the rmse
with cte as (
select
    to_char(time, 'YYYY-MM') as month,
    round(sum(engagement_rate)::numeric,1) as actual_value
from socmed_sc.twitter_tbl
group by 1
order by 1
), cte2 as (
select
    month, 
    actual_value, 
    coalesce(lag(actual_value, 1) over(order by month),0) as naive_forecast
from cte
)
select 
    round(cast(sqrt(avg(power((actual_value-naive_forecast), 2))) as numeric),2) as rmse
from cte2
















