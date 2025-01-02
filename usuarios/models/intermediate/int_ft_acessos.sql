WITH source AS (
    SELECT
        us.sk_dim AS sk_dim_usuarios,
        st.sk_dim AS sk_dim_sistemas,
        ng.sk_dim AS sk_dim_navegador,
        ac.sk_dim AS sk_dim_acao,
        ROW_NUMBER() OVER (
            ORDER BY dl.id
        ) AS ft_sk
    FROM
        {{ ref('stg_acessos') }} AS dl
    INNER JOIN
        {{ ref('int_dim_usuarios') }} AS us
        ON dl.id_usuario = us.cod_usuario
    INNER JOIN
        {{ ref('int_dim_sistemas') }} AS st
        ON dl.sistema_origem_tratado = st.desc_sistemas
    INNER JOIN
        {{ ref('int_dim_navegador') }} AS ng
        ON dl.navegador = ng.desc_navegador
    INNER JOIN
        {{ ref('int_dim_acao') }} AS ac
        ON dl.acao_realizada = ac.desc_acao

    ORDER BY dl.id ASC
)

SELECT *
FROM
    source
