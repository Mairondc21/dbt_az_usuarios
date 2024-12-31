WITH source AS(
    SELECT
        ng.DESC_NAVEGADOR,
        COUNT(ft.sk_dim_navegador) AS CONTADOR_NAVEGADOR
    FROM
        {{ ref('int_ft_acessos') }} ft
    INNER JOIN {{ ref('int_dim_navegador') }} ng ON ng.sk_dim = ft.sk_dim_navegador
    GROUP BY ng.DESC_NAVEGADOR
    
)
SELECT
    *
FROM
    source