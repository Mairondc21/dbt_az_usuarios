WITH source AS (
    SELECT
        'total' AS total,
        COUNT(ft_sk) AS total_legado,
        ROUND(AVG(dif_col), 2) AS media
    FROM {{ ref('int_ft_acessos_legado') }}
)


SELECT * FROM source
