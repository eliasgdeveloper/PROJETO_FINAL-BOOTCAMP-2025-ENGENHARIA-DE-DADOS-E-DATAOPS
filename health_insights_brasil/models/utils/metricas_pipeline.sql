-- Métricas de performance e monitoramento do pipeline dbt
{{ config(
    materialized='table',
    tags=['utils', 'monitoring']
) }}

WITH modelos_execucao AS (
    SELECT 
        'stg_sinasc' AS modelo,
        'staging' AS camada,
        (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }}) AS total_registros,
        (SELECT COUNT(DISTINCT codmunres) FROM {{ ref('stg_sinasc') }}) AS municipios_unicos,
        (SELECT MIN(data_nascimento) FROM {{ ref('stg_sinasc') }}) AS data_min,
        (SELECT MAX(data_nascimento) FROM {{ ref('stg_sinasc') }}) AS data_max,
        CURRENT_TIMESTAMP() AS executado_em
        
    UNION ALL
    
    SELECT 
        'fct_nascimentos' AS modelo,
        'marts' AS camada,
        (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }}) AS total_registros,
        (SELECT COUNT(DISTINCT sk_geografia) FROM {{ ref('fct_nascimentos') }}) AS geografias_unicas,
        (SELECT MIN(data_nascimento) FROM {{ ref('fct_nascimentos') }}) AS data_min,
        (SELECT MAX(data_nascimento) FROM {{ ref('fct_nascimentos') }}) AS data_max,
        CURRENT_TIMESTAMP() AS executado_em
        
    UNION ALL
    
    SELECT 
        'dim_geografia' AS modelo,
        'dimensions' AS camada,
        (SELECT COUNT(*) FROM {{ ref('dim_geografia') }}) AS total_registros,
        (SELECT COUNT(DISTINCT estado) FROM {{ ref('dim_geografia') }}) AS estados_unicos,
        NULL AS data_min,
        NULL AS data_max,
        CURRENT_TIMESTAMP() AS executado_em
        
    UNION ALL
    
    SELECT 
        'dim_demografia_mae' AS modelo,
        'dimensions' AS camada,
        (SELECT COUNT(*) FROM {{ ref('dim_demografia_mae') }}) AS total_registros,
        (SELECT COUNT(DISTINCT faixa_etaria_detalhada) FROM {{ ref('dim_demografia_mae') }}) AS faixas_etarias,
        NULL AS data_min,
        NULL AS data_max,
        CURRENT_TIMESTAMP() AS executado_em
        
    UNION ALL
    
    SELECT 
        'dim_medico' AS modelo,
        'dimensions' AS camada,
        (SELECT COUNT(*) FROM {{ ref('dim_medico') }}) AS total_registros,
        (SELECT COUNT(DISTINCT modalidade_parto) FROM {{ ref('dim_medico') }}) AS tipos_parto,
        NULL AS data_min,
        NULL AS data_max,
        CURRENT_TIMESTAMP() AS executado_em
),

qualidade_dados AS (
    SELECT 
        'Completude Geral' AS metrica,
        ROUND(
            (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }} 
             WHERE data_nascimento IS NOT NULL 
             AND codmunres IS NOT NULL 
             AND idade_mae IS NOT NULL) * 100.0 / 
            (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }}), 2
        ) AS valor_percentual,
        CASE 
            WHEN ROUND(
                (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }} 
                 WHERE data_nascimento IS NOT NULL 
                 AND codmunres IS NOT NULL 
                 AND idade_mae IS NOT NULL) * 100.0 / 
                (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }}), 2
            ) >= 95 THEN 'Excelente'
            WHEN ROUND(
                (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }} 
                 WHERE data_nascimento IS NOT NULL 
                 AND codmunres IS NOT NULL 
                 AND idade_mae IS NOT NULL) * 100.0 / 
                (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }}), 2
            ) >= 85 THEN 'Boa'
            ELSE 'Necessita Atenção'
        END AS avaliacao
        
    UNION ALL
    
    SELECT 
        'Dados Médicos Completos' AS metrica,
        ROUND(
            (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }} 
             WHERE peso IS NOT NULL 
             AND apgar5 IS NOT NULL 
             AND gestacao IS NOT NULL) * 100.0 / 
            (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }}), 2
        ) AS valor_percentual,
        CASE 
            WHEN ROUND(
                (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }} 
                 WHERE peso IS NOT NULL 
                 AND apgar5 IS NOT NULL 
                 AND gestacao IS NOT NULL) * 100.0 / 
                (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }}), 2
            ) >= 90 THEN 'Excelente'
            WHEN ROUND(
                (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }} 
                 WHERE peso IS NOT NULL 
                 AND apgar5 IS NOT NULL 
                 AND gestacao IS NOT NULL) * 100.0 / 
                (SELECT COUNT(*) FROM {{ ref('stg_sinasc') }}), 2
            ) >= 75 THEN 'Boa'
            ELSE 'Necessita Atenção'
        END AS avaliacao
        
    UNION ALL
    
    SELECT 
        'Integridade Referencial' AS metrica,
        ROUND(
            (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }} f
             JOIN {{ ref('dim_geografia') }} g ON f.sk_geografia = g.sk_geografia
             JOIN {{ ref('dim_demografia_mae') }} d ON f.sk_demografia = d.sk_demografia
             JOIN {{ ref('dim_medico') }} m ON f.sk_medico = m.sk_medico) * 100.0 / 
            (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }}), 2
        ) AS valor_percentual,
        CASE 
            WHEN ROUND(
                (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }} f
                 JOIN {{ ref('dim_geografia') }} g ON f.sk_geografia = g.sk_geografia
                 JOIN {{ ref('dim_demografia_mae') }} d ON f.sk_demografia = d.sk_demografia
                 JOIN {{ ref('dim_medico') }} m ON f.sk_medico = m.sk_medico) * 100.0 / 
                (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }}), 2
            ) = 100 THEN 'Perfeita'
            WHEN ROUND(
                (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }} f
                 JOIN {{ ref('dim_geografia') }} g ON f.sk_geografia = g.sk_geografia
                 JOIN {{ ref('dim_demografia_mae') }} d ON f.sk_demografia = d.sk_demografia
                 JOIN {{ ref('dim_medico') }} m ON f.sk_medico = m.sk_medico) * 100.0 / 
                (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }}), 2
            ) >= 95 THEN 'Excelente'
            ELSE 'Necessita Correção'
        END AS avaliacao
),

outliers_dados AS (
    SELECT 
        'Idade Materna' AS campo,
        COUNT(CASE WHEN idade_mae < 10 OR idade_mae > 60 THEN 1 END) AS outliers_detectados,
        ROUND(COUNT(CASE WHEN idade_mae < 10 OR idade_mae > 60 THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_outliers
    FROM {{ ref('stg_sinasc') }}
    
    UNION ALL
    
    SELECT 
        'Peso Nascimento' AS campo,
        COUNT(CASE WHEN peso < 300 OR peso > 6000 THEN 1 END) AS outliers_detectados,
        ROUND(COUNT(CASE WHEN peso < 300 OR peso > 6000 THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_outliers
    FROM {{ ref('stg_sinasc') }}
    
    UNION ALL
    
    SELECT 
        'Idade Gestacional' AS campo,
        COUNT(CASE WHEN gestacao < 20 OR gestacao > 45 THEN 1 END) AS outliers_detectados,
        ROUND(COUNT(CASE WHEN gestacao < 20 OR gestacao > 45 THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_outliers
    FROM {{ ref('stg_sinasc') }}
),

performance_transformacoes AS (
    SELECT 
        'Performance Geral' AS categoria,
        'Registros Processados' AS metrica,
        (SELECT SUM(total_registros) FROM modelos_execucao) AS valor_numerico,
        'registros' AS unidade
        
    UNION ALL
    
    SELECT 
        'Cobertura Geográfica' AS categoria,
        'Estados Cobertos' AS metrica,
        (SELECT COUNT(DISTINCT estado) FROM {{ ref('dim_geografia') }}) AS valor_numerico,
        'estados' AS unidade
        
    UNION ALL
    
    SELECT 
        'Diversidade Temporal' AS categoria,
        'Meses Analisados' AS metrica,
        (SELECT 
            DATEDIFF('month', MIN(data_nascimento), MAX(data_nascimento)) + 1 
         FROM {{ ref('fct_nascimentos') }}) AS valor_numerico,
        'meses' AS unidade
        
    UNION ALL
    
    SELECT 
        'Riqueza Analítica' AS categoria,
        'Dimensões Criadas' AS metrica,
        4 AS valor_numerico,  -- dim_tempo, dim_geografia, dim_demografia_mae, dim_medico
        'dimensões' AS unidade
)

-- Consolidação final das métricas
SELECT 
    'EXECUCAO_MODELOS' AS tipo_metrica,
    modelo AS dimensao_1,
    camada AS dimensao_2,
    CAST(total_registros AS STRING) AS dimensao_3,
    NULL AS dimensao_4,
    total_registros AS valor_numerico_1,
    municipios_unicos AS valor_numerico_2,
    NULL AS valor_numerico_3,
    executado_em AS timestamp_metrica,
    'ELIAS' AS criado_por
FROM modelos_execucao

UNION ALL

SELECT 
    'QUALIDADE_DADOS' AS tipo_metrica,
    metrica AS dimensao_1,
    avaliacao AS dimensao_2,
    CAST(valor_percentual AS STRING) AS dimensao_3,
    NULL AS dimensao_4,
    valor_percentual AS valor_numerico_1,
    NULL AS valor_numerico_2,
    NULL AS valor_numerico_3,
    CURRENT_TIMESTAMP() AS timestamp_metrica,
    'ELIAS' AS criado_por
FROM qualidade_dados

UNION ALL

SELECT 
    'OUTLIERS_DADOS' AS tipo_metrica,
    campo AS dimensao_1,
    NULL AS dimensao_2,
    CAST(perc_outliers AS STRING) AS dimensao_3,
    NULL AS dimensao_4,
    outliers_detectados AS valor_numerico_1,
    perc_outliers AS valor_numerico_2,
    NULL AS valor_numerico_3,
    CURRENT_TIMESTAMP() AS timestamp_metrica,
    'ELIAS' AS criado_por
FROM outliers_dados

UNION ALL

SELECT 
    'PERFORMANCE_PIPELINE' AS tipo_metrica,
    categoria AS dimensao_1,
    metrica AS dimensao_2,
    unidade AS dimensao_3,
    NULL AS dimensao_4,
    valor_numerico AS valor_numerico_1,
    NULL AS valor_numerico_2,
    NULL AS valor_numerico_3,
    CURRENT_TIMESTAMP() AS timestamp_metrica,
    'ELIAS' AS criado_por
FROM performance_transformacoes
