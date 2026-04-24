with base_commits as (
    select * from {{ ref('silver_commits') }}
),

final as (
    select
        *,
        -- Extração da hora para o heatmap do Looker
        extract(hour from commit_at) as hour_of_day,
        
        -- Métrica Humano vs Bot
        case 
            when is_bot = true then 'Bot'
            else 'Human'
        end as author_category,

        -- Dia da semana (útil para ver se trabalham no fds)
        format_timestamp('%A', commit_at) as day_of_week,

        -- Cálculo de frescor (dias desde o commit até hoje)
        date_diff(current_date(), date(commit_at), day) as days_since_commit

    from base_commits
)

select * from final