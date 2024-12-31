WITH source AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY dl.id) AS FT_SK,
        us.sk_dim AS SK_DIM_USUARIOS,
        st.sk_dim AS SK_DIM_SISTEMAS,
        ng.sk_dim AS SK_DIM_NAVEGADOR,
        ac.sk_dim AS SK_DIM_ACAO
    FROM
        {{ ref('stg_acessos') }} dl
    INNER JOIN {{ ref('int_dim_usuarios') }} us ON us.COD_USUARIO = dl.id_usuario
    INNER JOIN {{ ref('int_dim_sistemas') }} st ON st.DESC_SISTEMAS = dl.sistema_origem_tratado
    INNER JOIN {{ ref('int_dim_navegador') }} ng ON ng.DESC_NAVEGADOR = dl.navegador
    INNER JOIN {{ ref('int_dim_acao') }} ac ON ac.DESC_ACAO = dl.acao_realizada

    ORDER BY id ASC
)

SELECT
    *
FROM
    source