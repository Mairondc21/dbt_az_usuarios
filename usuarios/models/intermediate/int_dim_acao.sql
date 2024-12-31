WITH source AS (
    SELECT DISTINCT
        acao_realizada AS DESC_ACAO
    FROM
        {{ ref('stg_acessos') }}
),
increment_id AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY DESC_ACAO) AS SK_DIM,
        DESC_ACAO
    FROM
        source
)
SELECT
    *
FROM
    increment_id
    