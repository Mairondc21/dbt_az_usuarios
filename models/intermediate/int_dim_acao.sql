WITH source AS (
    SELECT DISTINCT acao_realizada AS desc_acao
    FROM
        {{ ref('stg_acessos') }}
),

increment_id AS (
    SELECT
        desc_acao,
        ROW_NUMBER() OVER (
            ORDER BY desc_acao
        ) AS sk_dim
    FROM
        source
)

SELECT *
FROM
    increment_id
