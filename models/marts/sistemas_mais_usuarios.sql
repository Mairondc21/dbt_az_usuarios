WITH source AS (
    SELECT
        st.desc_sistemas,
        COUNT(ft.sk_dim_usuarios) AS count_user
    FROM
        {{ ref('int_ft_acessos') }} AS ft
    INNER JOIN
        {{ ref('int_dim_sistemas') }} AS st
        ON ft.sk_dim_sistemas = st.sk_dim
    INNER JOIN
        {{ ref('int_dim_usuarios') }} AS us
        ON ft.sk_dim_usuarios = us.sk_dim
    GROUP BY st.desc_sistemas
)

SELECT *
FROM
    source
