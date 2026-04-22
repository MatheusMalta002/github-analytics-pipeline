with source as (
    select * from {{ source('bronze_github', 'issues') }}
),

renamed as (
    select
        -- 1. Chaves e IDs
        id as issue_id,
        
        -- 2. Referência
        number as issue_number,
        
        -- 3. Repositório
        repository as repository_name,

        -- 4. Conteúdo
        title,
        
        -- 5. Lógica Pull Request
        case 
            when pull_request is not null then true 
            else false 
        end as is_pull_request,

        -- 6. Status
        state,

        -- Extração do Usuário (JSON)
        -- 7. Avatar
        json_extract_scalar(user, '$.avatar_url') as author_avatar_url,
        -- 8. Nome do Usuário
        json_extract_scalar(user, '$.login') as author_handle,
        -- 9. Tipo (User/Bot)
        json_extract_scalar(user, '$.type') as author_type,
        
        -- 10. Lógica de Bot
        case 
            when json_extract_scalar(user, '$.type') = 'Bot' then true 
            else false 
        end as is_bot,

        -- 11. Relacionamento com o Repo
        author_association,

        -- 12. Popularidade
        comments as comment_count,

        -- 13. Categorização (mantemos como JSON para tratar se precisar)
        labels as labels_list,

        -- 14. Datas (Casting para Datetime)
        cast(created_at as datetime) as created_at,
        -- 15. Última atualização
        cast(updated_at as datetime) as updated_at,
        -- 16. Data de fechamento
        cast(closed_at as datetime) as closed_at,

        -- 17. Se o código entrou de fato (extraído do JSON pull_request)
        cast(json_extract_scalar(pull_request, '$.merged_at') as datetime) as merged_at,

        -- 18. Link Externo
        html_url

    from source
)

select * from renamed