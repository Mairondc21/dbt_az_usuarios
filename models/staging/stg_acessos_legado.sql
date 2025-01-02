WITH source AS (
    SELECT
        id,
        id_usuario,
        nome_usuario,
        sistema_origem,
        navegador,
        acao_realizada,
        dbt_valid_from,
        dbt_valid_to
    FROM
        {{ source('snapshots','usuario_snapshot') }}
),

tranform_string AS (
    SELECT
        *,
        CASE
            WHEN sistema_origem = 'Sitemas de compras' THEN 'Sistema de Compras'
            ELSE sistema_origem
        END AS sistema_origem_tratado
    FROM
        source
),

transform_date AS (
    SELECT
        *,
        to_char(dbt_valid_from, 'YYYY-MM-DD HH24:MI:SS') AS coluna_ativa_de,
        to_char(dbt_valid_to, 'YYYY-MM-DD HH24:MI:SS') AS coluna_ativa_ate
    FROM
        tranform_string
)

SELECT
    id,
    id_usuario,
    nome_usuario,
    sistema_origem_tratado,
    navegador,
    acao_realizada,
    coluna_ativa_de,
    coluna_ativa_ate
FROM
    transform_date
