WITH source AS (
    SELECT DISTINCT
        id_usuario AS cod_usuario,
        nome_usuario AS desc_usuario
    FROM
        {{ ref('stg_acessos') }}
),

increment_id AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY cod_usuario
        ) AS sk_dim
    FROM
        source
)

SELECT
    sk_dim,
    cod_usuario,
    desc_usuario
FROM
    increment_id
