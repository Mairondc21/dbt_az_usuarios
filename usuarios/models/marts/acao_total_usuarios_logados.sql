WITH source AS (
    SELECT
        ac.desc_acao,
        COUNT(ft.sk_dim_acao) AS COUNT_ACAO
    FROM {{ ref('int_ft_acessos') }} ft
    INNER JOIN {{ ref('int_dim_acao') }} ac ON ac.sk_dim = ft.sk_dim_acao
    GROUP BY ac.desc_acao
)

SELECT
    *
FROM
    source