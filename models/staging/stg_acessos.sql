WITH source AS (
    SELECT
        id,
        id_usuario,
        nome_usuario,
        sistema_origem,
        navegador,
        acao_realizada,
        dbt_valid_to AS coluna_ativa_ate
    FROM
        {{ source('snapshots','usuario_snapshot') }}
    WHERE dbt_valid_to IS NULL
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
)

SELECT
    id,
    id_usuario,
    nome_usuario,
    sistema_origem_tratado,
    navegador,
    acao_realizada
FROM
    tranform_string
