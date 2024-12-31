WITH source AS (
    SELECT DISTINCT
        navegador AS DESC_NAVEGADOR
    FROM
        {{ ref('stg_acessos') }}
),
increment_id AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY DESC_NAVEGADOR) AS SK_DIM,
        DESC_NAVEGADOR
    FROM
        source
)
SELECT
    *
FROM
    increment_id
    