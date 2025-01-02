WITH source AS (
    SELECT
        us.sk_dim AS sk_dim_usuarios,
        st.sk_dim AS sk_dim_sistemas,
        ng.sk_dim AS sk_dim_navegador,
        ac.sk_dim AS sk_dim_acao,
        dl.coluna_ativa_de,
        dl.coluna_ativa_ate,
        ROW_NUMBER() OVER (
            ORDER BY dl.id
        ) AS ft_sk
    FROM
        {{ ref('stg_acessos_legado') }} AS dl
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
),

filtered_source AS (
    SELECT
        *,
        coluna_ativa_de::timestamp AS coluna_ativa_de_tratada,
        coluna_ativa_ate::timestamp AS coluna_ativa_ate_tratada
    FROM
        source
),

transform_date AS (
    SELECT
        *,
        EXTRACT(
            minutes
            FROM coluna_ativa_ate_tratada - coluna_ativa_de_tratada
        ) AS dif_col
    FROM
        filtered_source
)

SELECT
    ft_sk,
    sk_dim_usuarios,
    sk_dim_sistemas,
    sk_dim_navegador,
    sk_dim_acao,
    coluna_ativa_de,
    coluna_ativa_ate,
    dif_col
FROM
    transform_date
