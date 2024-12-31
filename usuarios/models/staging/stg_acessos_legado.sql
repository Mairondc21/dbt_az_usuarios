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
        {{ ref('usuario_snapshot') }}
),
transform_date AS (
    SELECT
        *,
        to_char(dbt_valid_from, 'YYYY-MM-DD HH24:MI:SS') AS coluna_ativa_de,
        to_char(dbt_valid_TO, 'YYYY-MM-DD HH24:MI:SS') AS coluna_ativa_ate
    FROM
        source
)

SELECT
    id,
    id_usuario,
    nome_usuario,
    sistema_origem,
    navegador,
    acao_realizada,
    coluna_ativa_de,
    coluna_ativa_ate
FROM 
    transform_date
