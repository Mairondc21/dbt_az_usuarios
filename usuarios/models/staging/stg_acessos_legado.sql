WITH source AS (
    SELECT
        id,
        id_usuario,
        nome_usuario,
        sistema_origem,
        navegador,
        acao_realizada,
        dbt_valid_from AS coluna_ativa_de,
        dbt_valid_to AS coluna_ativa_ate
    FROM
        {{ ref('usuario_snapshot') }}
)

SELECT * FROM source