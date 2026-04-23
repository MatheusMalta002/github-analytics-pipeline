with source as (
    select * from {{ ref('stg_issues') }}
),

deduplicated as (
    select 
        *
    from source

    qualify row_number() over (
        partition by issue_id            -- Para cada ID de Issue repetido...
        order by updated_at desc         -- Ordene da atualização mais recente para a mais antiga...
    ) = 1                                -- E pegue apenas a primeira (a mais atual).
)

select * from deduplicated