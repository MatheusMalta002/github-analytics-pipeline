with source as (
    select * from {{ ref('stg_commits') }}
),

deduplicated as (
    select 
        *
    from source

    qualify row_number() over (
        partition by commit_sha         -- Para cada SHA de Commit repetido...
        order by commit_at desc         -- Ordene da data mais recente para a mais antiga...
    ) = 1                               -- E pegue apenas a primeira (a mais atual).
)

select * from deduplicated