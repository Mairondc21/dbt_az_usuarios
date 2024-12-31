WITH source AS (
    SELECT DISTINCT
        id_usuario AS COD_USUARIO,
        nome_usuario AS DESC_USUARIO
    FROM
        {{ ref('stg_acessos') }}
),
increment_id AS(
    SELECT
        *,
        ROW_NUMBER() OVER(ORDER BY COD_USUARIO) AS SK_DIM
    FROM
        source
)

SELECT 
    SK_DIM,
    COD_USUARIO,
    DESC_USUARIO
FROM 
    increment_id