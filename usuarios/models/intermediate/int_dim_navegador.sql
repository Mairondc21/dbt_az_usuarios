WITH source AS (
    SELECT DISTINCT navegador AS desc_navegador
    FROM
        {{ ref('stg_acessos') }}
),

increment_id AS (
    SELECT
        desc_navegador,
        ROW_NUMBER() OVER (
            ORDER BY desc_navegador
        ) AS sk_dim
    FROM
        source
)

SELECT *
FROM
    increment_id
