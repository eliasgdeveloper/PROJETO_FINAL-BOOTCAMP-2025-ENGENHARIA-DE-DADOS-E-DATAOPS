{{ config(
    materialized='view',
    description='Dados do SINASC limpos e padronizados'
) }}

WITH source_data AS (
    SELECT
        "contador"::INTEGER as id_nascimento,
        "IDADEMAE"::INTEGER as idade_mae,
        CASE 
            WHEN "SEXO"::INTEGER = 1 THEN 'Masculino'
            WHEN "SEXO"::INTEGER = 2 THEN 'Feminino'
            ELSE 'Ignorado'
        END as sexo,
        "PESO"::INTEGER as peso_nascimento,
        CASE 
            WHEN "PARTO"::INTEGER = 1 THEN 'Vaginal'
            WHEN "PARTO"::INTEGER = 2 THEN 'Cesáreo'
            ELSE 'Ignorado'
        END as tipo_parto,
        TRY_TO_DATE("DTNASC", 'DDMMYYYY') as data_nascimento,
        CURRENT_TIMESTAMP() as dw_criado_em
    FROM {{ source('raw_data', 'SINASC_RAW') }}
    WHERE "contador" IS NOT NULL
)

SELECT * FROM source_data
