WITH sources AS (
    SELECT
        id,
        id_usuario,
        nome_usuario,
        sistema_origem,
        navegador,
        acao_realizada,
        created_at
    FROM public.az_bucket
)

SELECT *
FROM sources
