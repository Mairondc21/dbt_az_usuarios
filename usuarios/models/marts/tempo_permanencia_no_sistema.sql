WITH source AS (
    SELECT
        'total' AS TOTAL,  
        COUNT(ft_sk) AS TOTAL_LEGADO,
        ROUND(AVG(dif_col),2) AS MEDIA
    FROM {{ ref('int_ft_acessos_legado')}}
)


SELECT * FROM source