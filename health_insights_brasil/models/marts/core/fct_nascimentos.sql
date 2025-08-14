-- Tabela fato principal com métricas calculadas e indicadores de saúde
{{ config(
    materialized='table',
    tags=['marts', 'core', 'facts']
) }}

WITH nascimentos_base AS (
    SELECT 
        *,
        -- Indicadores OMS
        CASE 
            WHEN peso < 1500 THEN 'Extremo Baixo Peso'
            WHEN peso < 2500 THEN 'Baixo Peso'
            WHEN peso > 4000 THEN 'Macrossomia'
            ELSE 'Peso Normal'
        END AS categoria_peso_oms,
        
        -- Classificação gestacional
        CASE 
            WHEN semanas_gestacao < 32 THEN 'Muito Prematuro'
            WHEN semanas_gestacao < 37 THEN 'Prematuro'
            WHEN semanas_gestacao > 42 THEN 'Pós-termo'
            ELSE 'A termo'
        END AS categoria_gestacional,
        
        -- Risco materno
        CASE 
            WHEN idade_mae < 18 THEN 'Alto Risco - Adolescente'
            WHEN idade_mae > 35 THEN 'Alto Risco - Idade Avançada'
            ELSE 'Baixo Risco'
        END AS risco_materno,
        
        -- Métricas calculadas
        CASE WHEN peso < 2500 THEN 1 ELSE 0 END AS flag_baixo_peso,
        CASE WHEN semanas_gestacao < 37 THEN 1 ELSE 0 END AS flag_prematuro,
        CASE WHEN consultas_prenatal < 7 THEN 1 ELSE 0 END AS flag_prenatal_inadequado,
        
        -- Score de risco composto (0-10)
        (
            CASE WHEN peso < 2500 THEN 3 ELSE 0 END +
            CASE WHEN semanas_gestacao < 37 THEN 2 ELSE 0 END +
            CASE WHEN idade_mae < 18 OR idade_mae > 35 THEN 2 ELSE 0 END +
            CASE WHEN consultas_prenatal < 4 THEN 2 ELSE 0 END +
            CASE WHEN tipo_parto = 'Cesáreo' THEN 1 ELSE 0 END
        ) AS score_risco_total

    FROM {{ ref('stg_sinasc') }}
    WHERE peso IS NOT NULL 
      AND idade_mae IS NOT NULL
),

metricas_por_municipio AS (
    SELECT 
        cod_municipio_nasc,
        municipio_nasc,
        uf,
        COUNT(*) AS total_nascimentos,
        AVG(peso) AS peso_medio,
        AVG(idade_mae) AS idade_mae_media,
        SUM(flag_baixo_peso) AS casos_baixo_peso,
        SUM(flag_prematuro) AS casos_prematuros,
        ROUND(AVG(score_risco_total), 2) AS score_risco_medio,
        
        -- Taxas percentuais
        ROUND((SUM(flag_baixo_peso) * 100.0 / COUNT(*)), 2) AS taxa_baixo_peso,
        ROUND((SUM(flag_prematuro) * 100.0 / COUNT(*)), 2) AS taxa_prematuridade,
        ROUND((SUM(flag_prenatal_inadequado) * 100.0 / COUNT(*)), 2) AS taxa_prenatal_inadequado
        
    FROM nascimentos_base
    GROUP BY cod_municipio_nasc, municipio_nasc, uf
)

SELECT 
    nb.*,
    mpm.peso_medio AS peso_medio_municipio,
    mpm.taxa_baixo_peso AS taxa_baixo_peso_municipio,
    mpm.score_risco_medio AS score_risco_municipio,
    
    -- Classificação do município
    CASE 
        WHEN mpm.taxa_baixo_peso > 15 THEN 'Crítico'
        WHEN mpm.taxa_baixo_peso > 10 THEN 'Alto Risco'
        WHEN mpm.taxa_baixo_peso > 5 THEN 'Moderado'
        ELSE 'Baixo Risco'
    END AS classificacao_municipio,
    
    -- Data de processamento
    CURRENT_TIMESTAMP() AS data_processamento

FROM nascimentos_base nb
LEFT JOIN metricas_por_municipio mpm 
    ON nb.cod_municipio_nasc = mpm.cod_municipio_nasc
