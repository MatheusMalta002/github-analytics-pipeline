with source as (
    select * from {{ ref('stg_comments') }}
),

deduplicated as (
    select 
        *
    from source

    qualify row_number() over (
        partition by comment_id          -- Para cada ID de Comentário repetido...
        order by updated_at desc         -- Ordene da atualização mais recente para a mais antiga...
    ) = 1                                -- E pegue apenas a primeira (a mais atual).
)

select * from deduplicated