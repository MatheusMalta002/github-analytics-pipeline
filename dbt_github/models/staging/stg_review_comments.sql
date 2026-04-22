with source as (
    select * from {{ source('bronze_github', 'review_comments') }}
),

renamed as (
    select
        -- 1. Chaves e IDs
        id as review_comment_id,

        -- 2. Repositório
        repository as repository_name,

        -- 3. Referência (Extração do número do PR via URL)
        safe_cast(split(pull_request_url, '/')[safe_offset(array_length(split(pull_request_url, '/')) - 1)] as int64) as pull_request_number,

        -- Extração do Usuário (JSON)
        -- 4. Nome do Usuário
        json_extract_scalar(user, '$.login') as author_handle,
        -- 5. Avatar
        json_extract_scalar(user, '$.avatar_url') as author_avatar_url,
        
        -- 6. Lógica de Bot
        case 
            when json_extract_scalar(user, '$.type') = 'Bot' then true 
            else false 
        end as is_bot,

        -- 7. Conteúdo
        body as comment_content,
        -- 8. Caminho do Arquivo
        path as file_path,

        -- 9. Hierarquia (Identificar se é resposta)
        in_reply_to_id as parent_comment_id,

        -- 10. Popularidade (Reações)
        safe_cast(json_extract_scalar(reactions, '$.total_count') as int64) as reaction_count,

        -- 11. Relacionamento com o Repo
        author_association,

        -- 12. Datas (Casting para Datetime)
        cast(created_at as timestamp) as created_at,
        -- 13. Última atualização
        cast(updated_at as timestamp) as updated_at,

        -- 14. Link Externo
        html_url

    from source
)

select * from renamed