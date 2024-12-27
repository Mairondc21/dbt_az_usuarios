{% snapshot usuario_snapshot %}

{{
    config(
        target_schema='snapshots',
        target_database='postgres',
        unique_key='id',
        strategy='check',
        check_cols=['acao_realizada']
    )
}}
SELECT
    *
FROM {{ ref('raw_dl_acessos') }}
{% endsnapshot %}