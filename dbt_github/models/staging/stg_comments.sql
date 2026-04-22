with source as (
    select * from {{ source('bronze_github', 'comments') }}
),

renamed as (
    select
        -- 1. Chave Primária
        id as comment_id,

        -- 2. Repositório
        repository as repository_name,

        -- Extração do Usuário (JSON user)
        -- 3. ID do Autor
        json_extract_scalar(user, '$.id') as author_id,
        -- 4. Nome do Usuário
        json_extract_scalar(user, '$.login') as author_handle,
        -- 5. Tipo do Usuário
        json_extract_scalar(user, '$.type') as author_type,
        -- 6. Foto do Usuário
        json_extract_scalar(user, '$.avatar_url') as author_avatar_url,

        -- 7. Lógica de Bot
        case 
            when json_extract_scalar(user, '$.type') = 'Bot' then true 
            else false 
        end as is_bot,

        -- 8. Conteúdo do Comentário
        body as comment_content,

        -- 9. Chave Estrangeira (Extração do número da issue via URL)
        safe_cast(split(issue_url, '/')[safe_offset(array_length(split(issue_url, '/')) - 1)] as int64) as issue_number,

        -- 10. Engajamento (Reações)
        safe_cast(json_extract_scalar(reactions, '$.total_count') as int64) as reaction_count,

        -- 11. Data de Criação (Casting para Datetime)
        cast(created_at as timestamp) as created_at,

        -- 12. Última Atualização (Casting para Datetime)
        cast(updated_at as timestamp) as updated_at,

        -- 13. Link Externo
        html_url

    from source
)

select * from renamed