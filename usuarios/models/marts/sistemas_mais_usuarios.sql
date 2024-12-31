WITH source AS (
    SELECT
        st.desc_sistemas,
        COUNT(ft.sk_dim_usuarios) AS COUNT_USER
    FROM
        {{ ref('int_ft_acessos') }} ft
    INNER JOIN {{ ref('int_dim_sistemas') }} st ON st.sk_dim = ft.sk_dim_sistemas
    INNER JOIN {{ ref('int_dim_usuarios') }} us ON us.sk_dim = ft.sk_dim_usuarios
    GROUP BY st.desc_sistemas
)

SELECT
    *
FROM
    source