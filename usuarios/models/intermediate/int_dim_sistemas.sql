WITH source AS (
    SELECT DISTINCT
        sistema_origem_tratado AS DESC_SISTEMAS
    FROM
        {{ ref('stg_acessos') }}
),
increment_id AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY DESC_SISTEMAS) AS SK_DIM,
        DESC_SISTEMAS
    FROM
        source
    GROUP BY DESC_SISTEMAS
)
SELECT
    *
FROM
    increment_id


    