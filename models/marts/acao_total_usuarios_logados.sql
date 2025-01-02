WITH source AS (
    SELECT
        ac.desc_acao,
        COUNT(ft.sk_dim_acao) AS count_acao
    FROM {{ ref('int_ft_acessos') }} AS ft
    INNER JOIN {{ ref('int_dim_acao') }} AS ac ON ft.sk_dim_acao = ac.sk_dim
    GROUP BY ac.desc_acao
)

SELECT *
FROM
    source
