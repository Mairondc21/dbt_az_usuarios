WITH source AS (
    SELECT DISTINCT sistema_origem_tratado AS desc_sistemas
    FROM
        {{ ref('stg_acessos') }}
),

increment_id AS (
    SELECT
        desc_sistemas,
        ROW_NUMBER() OVER (
            ORDER BY desc_sistemas
        ) AS sk_dim
    FROM
        source
    GROUP BY desc_sistemas
)

SELECT *
FROM
    increment_id
