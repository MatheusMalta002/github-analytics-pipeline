with all_authors as (
    -- Pegando autores de Issues e PRs
    select author_handle, author_avatar_url, author_type, created_at 
    from {{ ref('silver_issues') }}
    
    union all
    
    -- Pegando autores de Commits
    select author_handle, author_avatar_url, author_type, commit_at as created_at 
    from {{ ref('silver_commits') }}
    
    union all
    
    -- Pegando autores de Comentários Gerais
    select author_handle, author_avatar_url, author_type, created_at 
    from {{ ref('silver_comments') }}
    
    union all
    
    -- Pegando autores de Code Review (Não tem author_type, então marcamos como 'USER')
    select author_handle, author_avatar_url, 'USER' as author_type, created_at 
    from {{ ref('silver_review_comments') }}
),

contributor_stats as (
    select
        author_handle,
        max(author_avatar_url) as author_avatar_url,
        max(author_type) as author_type,
        min(created_at) as first_activity_at,
        max(created_at) as last_activity_at,
        count(*) as total_contributions_count
    from all_authors
    where author_handle is not null
    group by 1
),

final as (
    select
        *,
        -- Métrica de tempo de casa em dias
        timestamp_diff(last_activity_at, first_activity_at, day) as tenure_days,
        
        -- Classificação de Engajamento
        case 
            when total_contributions_count > 100 then 'Core Contributor'
            when total_contributions_count > 20 then 'Regular Contributor'
            else 'Casual Contributor'
        end as contributor_level
    from contributor_stats
)

select * from final