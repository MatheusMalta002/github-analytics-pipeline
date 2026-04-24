with general_comments as (
    select
        comment_id as interaction_id,
        repository_name,
        issue_number,
        author_handle,
        author_avatar_url,
        is_bot,
        comment_content,
        reaction_count,
        created_at,
        'General Comment' as interaction_type,
        html_url
    from {{ ref('silver_comments') }}
),

review_comments as (
    select
        review_comment_id as interaction_id,
        repository_name,
        pull_request_number as issue_number, -- normalizando para issue_number
        author_handle,
        author_avatar_url,
        is_bot,
        comment_content,
        reaction_count,
        created_at,
        'Code Review' as interaction_type,
        html_url
    from {{ ref('silver_review_comments') }}
),

final as (
    select * from general_comments
    union all
    select * from review_comments
)

select 
    *,
    extract(hour from created_at) as hour_of_day,
    format_timestamp('%A', created_at) as day_of_week
from final