with
    cost_per_engage as (
        select channel, sum(impressions) as `impressions`
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
        group by channel
        union all
        select channel, sum(impressions) as `impressions`
        from {{ ref("src_ads_creative_facebook_all_data") }}
        group by channel
        union all
        select channel, sum(imps) as `impressions`
        from {{ ref("src_ads_bing_all_data") }}
        group by channel
        union all
        select channel, 0 as `cost per engage`
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
        group by channel
    )
select * from cost_per_engage

Select * from {{ ref("src_ads_bing_all_data") }}