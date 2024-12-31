WITH source AS (
    SELECT DISTINCT
        sistema_origem
    FROM
        {{ ref('stg_acessos') }}
),
tranform_string AS (
    SELECT 
        CASE
            WHEN sistema_origem = 'Sitemas de compras' THEN 'Sistema de Compras'
            ELSE sistema_origem 
        END AS DESC_SISTEMAS
    FROM
        source
),
increment_id AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY DESC_SISTEMAS) AS SK_DIM,
        DESC_SISTEMAS
    FROM
        tranform_string
    GROUP BY DESC_SISTEMAS
)
SELECT
    *
FROM
    increment_id


    