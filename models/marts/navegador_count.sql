WITH source AS (
    SELECT
        ng.desc_navegador,
        COUNT(ft.sk_dim_navegador) AS contador_navegador
    FROM
        {{ ref('int_ft_acessos') }} AS ft
    INNER JOIN
        {{ ref('int_dim_navegador') }} AS ng
        ON ft.sk_dim_navegador = ng.sk_dim
    GROUP BY ng.desc_navegador

)

SELECT *
FROM
    source
