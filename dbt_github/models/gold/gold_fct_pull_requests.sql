with issues as (
    select * from {{ ref('silver_issues') }}
    where is_pull_request = true
),

-- Agregando comentários gerais
comment_stats as (
    select
        repository_name,
        issue_number,
        count(distinct comment_id) as total_comments,
        sum(reaction_count) as total_comment_reactions
    from {{ ref('silver_comments') }}
    group by 1, 2
),

-- Agregando comentários de revisão (code review)
review_stats as (
    select
        repository_name,
        pull_request_number as issue_number,
        count(distinct review_comment_id) as total_review_comments,
        sum(reaction_count) as total_review_reactions
    from {{ ref('silver_review_comments') }}
    group by 1, 2
)

select
    i.*,
    
    -- Métricas de Tempo (Calculadas em dias e horas)
    timestamp_diff(i.closed_at, i.created_at, day) as days_to_close,
    case 
        when timestamp_diff(i.closed_at, i.created_at, hour) < 24 then true 
        else false 
    end as is_fast_merge,

    -- Métricas de Engajamento
    coalesce(c.total_comments, 0) as total_general_comments,
    coalesce(r.total_review_comments, 0) as total_review_comments,
    (coalesce(c.total_comments, 0) + coalesce(r.total_review_comments, 0)) as interaction_volume,
    (coalesce(c.total_comment_reactions, 0) + coalesce(r.total_review_reactions, 0)) as total_reactions,

    -- Flags de Status e Saúde
    case 
        when i.merged_at is not null then 'Merged'
        when i.closed_at is not null and i.merged_at is null then 'Closed (Rejected)'
        else 'Open'
    end as detailed_merge_status,

    case 
        when i.state = 'open' and timestamp_diff(current_timestamp(), i.created_at, day) > 30 then true 
        else false 
    end as is_stale_30d,

    case 
        when (coalesce(c.total_comments, 0) + coalesce(r.total_review_comments, 0)) = 0 then true 
        else false 
    end as has_zero_comments

from issues i
left join comment_stats c 
    on i.repository_name = c.repository_name 
    and i.issue_number = c.issue_number
left join review_stats r 
    on i.repository_name = r.repository_name 
    and i.issue_number = r.issue_number