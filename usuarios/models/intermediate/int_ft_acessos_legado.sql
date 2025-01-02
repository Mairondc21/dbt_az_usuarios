WITH source AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY dl.id) AS FT_SK,
        us.sk_dim AS SK_DIM_USUARIOS,
        st.sk_dim AS SK_DIM_SISTEMAS,
        ng.sk_dim AS SK_DIM_NAVEGADOR,
        ac.sk_dim AS SK_DIM_ACAO,
        dl.coluna_ativa_de,
        dl.coluna_ativa_ate
    FROM
        {{ ref('stg_acessos_legado') }} dl
    INNER JOIN {{ ref('int_dim_usuarios') }} us ON us.COD_USUARIO = dl.id_usuario
    INNER JOIN {{ ref('int_dim_sistemas') }} st ON st.DESC_SISTEMAS = dl.sistema_origem_tratado
    INNER JOIN {{ ref('int_dim_navegador') }} ng ON ng.DESC_NAVEGADOR = dl.navegador
    INNER JOIN {{ ref('int_dim_acao') }} ac ON ac.DESC_ACAO = dl.acao_realizada

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
            MINUTES 
            FROM coluna_ativa_ate_tratada - coluna_ativa_de_tratada
        ) AS DIF_COL
    FROM
        filtered_source
)

SELECT
    FT_SK,
    SK_DIM_USUARIOS,
    SK_DIM_SISTEMAS,
    SK_DIM_NAVEGADOR,
    SK_DIM_ACAO,
    coluna_ativa_de,
    coluna_ativa_ate,
    DIF_COL
FROM
    transform_date