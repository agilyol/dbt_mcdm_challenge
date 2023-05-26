with
    cpc as (
        select channel, round(sum(spend) / sum(clicks), 2) as `cpc`
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
        group by channel
        union all
        select channel, round(sum(spend) / sum(clicks), 2) as `cpc`
        from {{ ref("src_ads_creative_facebook_all_data") }}
        group by channel
        union all
        select channel, round(sum(spend) / sum(clicks), 2) as `cpc`
        from {{ ref("src_ads_bing_all_data") }}
        group by channel
        union all
        select channel, round(sum(spend) / sum(clicks), 2) as `cpc`
        from {{ ref("src_ads_tiktok_ads_all_data") }}
        group by channel
    )
select * from cpc
