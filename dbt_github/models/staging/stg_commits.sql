with source as (
    select * from {{ source('bronze_github', 'commits') }}
),

renamed as (
    select
        -- 1. Chave Primária
        sha as commit_sha,
        
        -- 2. Repositório
        repository as repository_name,
        
        -- 3. Contexto de desenvolvimento
        branch,

        -- Extração do Autor (JSON author)
        -- 4. Nome do Usuário
        json_extract_scalar(author, '$.login') as author_handle,
        -- 5. ID do Autor
        json_extract_scalar(author, '$.id') as author_id,
        -- 6. Foto do Usuário
        json_extract_scalar(author, '$.avatar_url') as author_avatar_url,
        -- 7. Tipo do Autor
        json_extract_scalar(author, '$.type') as author_type,
        
        -- 8. Lógica de Bot
        case 
            when json_extract_scalar(author, '$.type') = 'Bot' then true 
            else false 
        end as is_bot,

        -- Extração do Commit (JSON commit)
        -- 9. Mensagem do Commit
        json_extract_scalar(commit, '$.message') as commit_message,
        -- 10. Data da Autoria (Casting para Datetime)
        cast(json_extract_scalar(commit, '$.author.date') as datetime) as commit_at,

        -- 11. Data de Registro (Casting para Datetime)
        cast(created_at as datetime) as created_at,

        -- 12. Link Externo
        html_url

    from source
)

select * from renamed