with
    conversation_cost_per_channel as (
        select channel, round(sum(spend) / sum(purchase), 2) as `conversion_cost`
        from {{ ref("src_ads_creative_facebook_all_data") }}
        group by channel
        union all
        select channel, round(sum(spend) / sum(conversions), 2) as `conversion_cost`
        from {{ ref("src_ads_tiktok_ads_all_data") }}
        group by channel
        union all
        select channel, round(sum(spend) / sum(conv), 2) as `conversion_cost`
        from {{ ref("src_ads_bing_all_data") }}
        group by channel
        union all
        select channel, 0 as `conversion_cost`
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
        group by channel
    )
select *
from conversation_cost_per_channel
