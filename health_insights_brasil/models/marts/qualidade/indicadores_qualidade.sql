-- Análise de indicadores de qualidade assistencial e performance hospitalar
{{ config(
    materialized='table',
    tags=['marts', 'qualidade']
) }}

WITH base_qualidade AS (
    SELECT 
        f.*,
        dt.ano,
        dt.mes,
        dg.estado,
        dg.regiao,
        dg.eh_capital,
        dd.idade_mae,
        dd.faixa_etaria_detalhada,
        dd.adequacao_prenatal,
        dd.vulnerabilidade_social,
        dm.modalidade_parto,
        dm.categoria_idade_gestacional,
        dm.categoria_peso,
        dm.nivel_risco_neonatal,
        dm.descricao_robson,
        dm.adequacao_consultas,
        dm.eh_cesareo,
        dm.eh_prematuro,
        dm.prenatal_adequado,
        dm.apgar5_normal
    FROM {{ ref('fct_nascimentos') }} f
    JOIN {{ ref('dim_tempo') }} dt ON f.data_nascimento = dt.data_nascimento
    JOIN {{ ref('dim_geografia') }} dg ON f.sk_geografia = dg.sk_geografia
    JOIN {{ ref('dim_demografia_mae') }} dd ON f.sk_demografia = dd.sk_demografia
    JOIN {{ ref('dim_medico') }} dm ON f.sk_medico = dm.sk_medico
),

-- Indicadores de qualidade por estado/região
qualidade_por_regiao AS (
    SELECT 
        estado,
        regiao,
        
        COUNT(*) AS total_nascimentos,
        
        -- Indicador 1: Taxa de Cesárea (meta OMS: 10-15%)
        COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*) AS taxa_cesarea,
        CASE 
            WHEN COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*) BETWEEN 10 AND 15 THEN 'Adequada'
            WHEN COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*) < 10 THEN 'Baixa'
            ELSE 'Excessiva'
        END AS avaliacao_cesarea,
        
        -- Indicador 2: Taxa de Prematuridade (meta: <10%)
        COUNT(CASE WHEN eh_prematuro THEN 1 END) * 100.0 / COUNT(*) AS taxa_prematuridade,
        CASE 
            WHEN COUNT(CASE WHEN eh_prematuro THEN 1 END) * 100.0 / COUNT(*) < 10 THEN 'Adequada'
            WHEN COUNT(CASE WHEN eh_prematuro THEN 1 END) * 100.0 / COUNT(*) < 15 THEN 'Limítrofe'
            ELSE 'Elevada'
        END AS avaliacao_prematuridade,
        
        -- Indicador 3: Cobertura Pré-natal Adequada (meta: >80%)
        COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) AS cobertura_prenatal,
        CASE 
            WHEN COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) >= 80 THEN 'Adequada'
            WHEN COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) >= 60 THEN 'Intermediária'
            ELSE 'Inadequada'
        END AS avaliacao_prenatal,
        
        -- Indicador 4: Taxa de Baixo Peso (meta: <10%)
        COUNT(CASE WHEN categoria_peso LIKE '%Baixo Peso%' THEN 1 END) * 100.0 / COUNT(*) AS taxa_baixo_peso,
        CASE 
            WHEN COUNT(CASE WHEN categoria_peso LIKE '%Baixo Peso%' THEN 1 END) * 100.0 / COUNT(*) < 10 THEN 'Adequada'
            ELSE 'Elevada'
        END AS avaliacao_baixo_peso,
        
        -- Indicador 5: APGAR 5min Normal (meta: >95%)
        COUNT(CASE WHEN apgar5_normal THEN 1 END) * 100.0 / COUNT(*) AS taxa_apgar5_normal,
        CASE 
            WHEN COUNT(CASE WHEN apgar5_normal THEN 1 END) * 100.0 / COUNT(*) >= 95 THEN 'Excelente'
            WHEN COUNT(CASE WHEN apgar5_normal THEN 1 END) * 100.0 / COUNT(*) >= 90 THEN 'Adequada'
            ELSE 'Necessita Melhoria'
        END AS avaliacao_apgar5,
        
        -- Indicador Composto de Qualidade
        (CASE WHEN COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*) BETWEEN 10 AND 15 THEN 25 ELSE 0 END +
         CASE WHEN COUNT(CASE WHEN eh_prematuro THEN 1 END) * 100.0 / COUNT(*) < 10 THEN 25 ELSE 0 END +
         CASE WHEN COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) >= 80 THEN 25 ELSE 0 END +
         CASE WHEN COUNT(CASE WHEN apgar5_normal THEN 1 END) * 100.0 / COUNT(*) >= 95 THEN 25 ELSE 0 END) AS score_qualidade,
         
        -- Peso médio ao nascer
        AVG(peso_nascimento) AS peso_medio,
        
        -- Idade materna média
        AVG(idade_mae) AS idade_mae_media
        
    FROM base_qualidade
    GROUP BY estado, regiao
),

-- Análise por Grupos de Robson (classificação internacional)
qualidade_robson AS (
    SELECT 
        descricao_robson,
        
        COUNT(*) AS total_nascimentos,
        COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*) AS taxa_cesarea_grupo,
        
        -- Contribuição para taxa geral de cesáreas
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM base_qualidade) AS contribuicao_populacao,
        COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / 
        (SELECT COUNT(CASE WHEN eh_cesareo THEN 1 END) FROM base_qualidade) AS contribuicao_cesareas,
        
        -- Outcomes por grupo
        AVG(peso_nascimento) AS peso_medio_grupo,
        COUNT(CASE WHEN apgar5_normal THEN 1 END) * 100.0 / COUNT(*) AS taxa_apgar5_normal_grupo,
        
        -- Classificação do grupo
        CASE 
            WHEN descricao_robson LIKE '%trabalho espontâneo%' THEN 'Baixo Risco'
            WHEN descricao_robson LIKE '%cesárea prévia%' OR descricao_robson LIKE '%pélvico%' THEN 'Alto Risco'
            WHEN descricao_robson LIKE '%Múltiplas%' OR descricao_robson LIKE '%Prematuros%' THEN 'Muito Alto Risco'
            ELSE 'Risco Intermediário'
        END AS categoria_risco_robson
        
    FROM base_qualidade
    WHERE descricao_robson != 'Não Classificado'
    GROUP BY descricao_robson
),

-- Análise temporal da qualidade
tendencia_qualidade AS (
    SELECT 
        ano,
        mes,
        
        COUNT(*) AS nascimentos_mes,
        
        -- Indicadores mensais
        COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*) AS taxa_cesarea_mes,
        COUNT(CASE WHEN eh_prematuro THEN 1 END) * 100.0 / COUNT(*) AS taxa_prematuridade_mes,
        COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) AS cobertura_prenatal_mes,
        AVG(peso_nascimento) AS peso_medio_mes,
        
        -- Variações em relação ao mês anterior
        LAG(COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*)) 
        OVER (ORDER BY ano, mes) AS taxa_cesarea_mes_anterior,
        
        -- Tendência (melhorando/piorando)
        CASE 
            WHEN COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*) < 
                 LAG(COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*)) 
                 OVER (ORDER BY ano, mes) THEN 'Melhorando'
            WHEN COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*) > 
                 LAG(COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*)) 
                 OVER (ORDER BY ano, mes) THEN 'Piorando'
            ELSE 'Estável'
        END AS tendencia_cesarea
        
    FROM base_qualidade
    GROUP BY ano, mes
),

-- Análise por vulnerabilidade social
qualidade_por_vulnerabilidade AS (
    SELECT 
        vulnerabilidade_social,
        
        COUNT(*) AS total_nascimentos,
        
        -- Indicadores por vulnerabilidade
        COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*) AS taxa_cesarea,
        COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) AS cobertura_prenatal,
        COUNT(CASE WHEN eh_prematuro THEN 1 END) * 100.0 / COUNT(*) AS taxa_prematuridade,
        AVG(peso_nascimento) AS peso_medio,
        COUNT(CASE WHEN apgar5_normal THEN 1 END) * 100.0 / COUNT(*) AS taxa_apgar5_normal,
        
        -- Gap em relação ao grupo de menor vulnerabilidade
        COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) - 
        (SELECT COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) 
         FROM base_qualidade WHERE vulnerabilidade_social = 'Baixa Vulnerabilidade') AS gap_prenatal,
         
        AVG(peso_nascimento) - 
        (SELECT AVG(peso_nascimento) 
         FROM base_qualidade WHERE vulnerabilidade_social = 'Baixa Vulnerabilidade') AS gap_peso,
         
        -- Classificação de equidade
        CASE 
            WHEN vulnerabilidade_social = 'Baixa Vulnerabilidade' THEN 'Referência'
            WHEN ABS(COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) - 
                     (SELECT COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*) 
                      FROM base_qualidade WHERE vulnerabilidade_social = 'Baixa Vulnerabilidade')) < 10 
            THEN 'Equidade Adequada'
            ELSE 'Inequidade Significativa'
        END AS nivel_equidade
        
    FROM base_qualidade
    GROUP BY vulnerabilidade_social
)

-- Consolidação final dos indicadores de qualidade
SELECT 
    'QUALIDADE_REGIONAL' AS tipo_indicador,
    estado AS dimensao_1,
    regiao AS dimensao_2,
    NULL AS dimensao_3,
    NULL AS dimensao_4,
    total_nascimentos,
    ROUND(taxa_cesarea, 2) AS indicador_1,
    ROUND(taxa_prematuridade, 2) AS indicador_2,
    ROUND(cobertura_prenatal, 2) AS indicador_3,
    score_qualidade AS indicador_4,
    avaliacao_cesarea AS avaliacao_1,
    avaliacao_prematuridade AS avaliacao_2,
    avaliacao_prenatal AS avaliacao_3,
    CURRENT_TIMESTAMP() AS data_analise
FROM qualidade_por_regiao

UNION ALL

SELECT 
    'QUALIDADE_ROBSON' AS tipo_indicador,
    descricao_robson AS dimensao_1,
    categoria_risco_robson AS dimensao_2,
    NULL AS dimensao_3,
    NULL AS dimensao_4,
    total_nascimentos,
    ROUND(taxa_cesarea_grupo, 2) AS indicador_1,
    ROUND(contribuicao_populacao, 2) AS indicador_2,
    ROUND(contribuicao_cesareas, 2) AS indicador_3,
    ROUND(taxa_apgar5_normal_grupo, 2) AS indicador_4,
    NULL AS avaliacao_1,
    NULL AS avaliacao_2,
    NULL AS avaliacao_3,
    CURRENT_TIMESTAMP() AS data_analise
FROM qualidade_robson

UNION ALL

SELECT 
    'QUALIDADE_TEMPORAL' AS tipo_indicador,
    CAST(ano AS STRING) AS dimensao_1,
    CAST(mes AS STRING) AS dimensao_2,
    tendencia_cesarea AS dimensao_3,
    NULL AS dimensao_4,
    nascimentos_mes AS total_nascimentos,
    ROUND(taxa_cesarea_mes, 2) AS indicador_1,
    ROUND(taxa_prematuridade_mes, 2) AS indicador_2,
    ROUND(cobertura_prenatal_mes, 2) AS indicador_3,
    ROUND(peso_medio_mes, 1) AS indicador_4,
    NULL AS avaliacao_1,
    NULL AS avaliacao_2,
    NULL AS avaliacao_3,
    CURRENT_TIMESTAMP() AS data_analise
FROM tendencia_qualidade

UNION ALL

SELECT 
    'QUALIDADE_EQUIDADE' AS tipo_indicador,
    vulnerabilidade_social AS dimensao_1,
    nivel_equidade AS dimensao_2,
    NULL AS dimensao_3,
    NULL AS dimensao_4,
    total_nascimentos,
    ROUND(taxa_cesarea, 2) AS indicador_1,
    ROUND(cobertura_prenatal, 2) AS indicador_2,
    ROUND(gap_prenatal, 2) AS indicador_3,
    ROUND(gap_peso, 1) AS indicador_4,
    NULL AS avaliacao_1,
    NULL AS avaliacao_2,
    NULL AS avaliacao_3,
    CURRENT_TIMESTAMP() AS data_analise
FROM qualidade_por_vulnerabilidade
