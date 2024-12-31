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
        {{ ref('usuario_snapshot') }}
    WHERE dbt_valid_to IS NULL
)

SELECT * FROM source