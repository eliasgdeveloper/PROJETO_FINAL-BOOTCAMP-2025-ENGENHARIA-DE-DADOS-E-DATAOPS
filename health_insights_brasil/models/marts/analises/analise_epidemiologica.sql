-- Análise epidemiológica e de saúde pública
{{ config(
    materialized='table',
    tags=['marts', 'analises']
) }}

WITH indicadores_base AS (
    SELECT 
        f.*,
        dt.ano,
        dt.mes,
        dt.trimestre,
        dt.estacao,
        dg.estado,
        dg.regiao,
        dg.eh_capital,
        dg.nivel_desenvolvimento,
        dd.idade_mae,
        dd.faixa_etaria_detalhada,
        dd.nivel_escolaridade,
        dd.raca_cor,
        dd.vulnerabilidade_social,
        dd.gestacao_alto_risco,
        dm.modalidade_parto,
        dm.categoria_idade_gestacional,
        dm.categoria_peso,
        dm.nivel_risco_neonatal,
        dm.complexidade_parto,
        dm.eh_cesareo,
        dm.eh_prematuro,
        dm.prenatal_adequado
    FROM {{ ref('fct_nascimentos') }} f
    JOIN {{ ref('dim_tempo') }} dt ON f.data_nascimento = dt.data_nascimento
    JOIN {{ ref('dim_geografia') }} dg ON f.sk_geografia = dg.sk_geografia
    JOIN {{ ref('dim_demografia_mae') }} dd ON f.sk_demografia = dd.sk_demografia
    JOIN {{ ref('dim_medico') }} dm ON f.sk_medico = dm.sk_medico
),

-- Análise de desigualdades sociais em saúde
desigualdades_sociais AS (
    SELECT 
        regiao,
        raca_cor,
        nivel_escolaridade,
        vulnerabilidade_social,
        
        COUNT(*) AS total_nascimentos,
        
        -- Indicadores de acesso e qualidade
        AVG(CASE WHEN prenatal_adequado THEN 1.0 ELSE 0.0 END) AS taxa_prenatal_adequado,
        AVG(CASE WHEN eh_cesareo THEN 1.0 ELSE 0.0 END) AS taxa_cesarea,
        AVG(CASE WHEN eh_prematuro THEN 1.0 ELSE 0.0 END) AS taxa_prematuridade,
        AVG(CASE WHEN categoria_peso LIKE '%Baixo Peso%' THEN 1.0 ELSE 0.0 END) AS taxa_baixo_peso,
        
        -- Outcomes neonatais
        AVG(peso_nascimento) AS peso_medio,
        AVG(apgar5) AS apgar5_medio,
        AVG(semanas_gestacao) AS ig_media,
        
        -- Indicador composto de qualidade
        AVG(CASE 
            WHEN prenatal_adequado AND apgar5 >= 7 AND peso_nascimento >= 2500 
            THEN 1.0 ELSE 0.0 
        END) AS taxa_outcome_otimo,
        
        -- Classificação de risco social
        CASE 
            WHEN vulnerabilidade_social = 'Alta Vulnerabilidade' 
                 AND nivel_escolaridade IN ('Fundamental Incompleto', 'Não Informado')
            THEN 'Grupo de Alto Risco Social'
            WHEN vulnerabilidade_social = 'Baixa Vulnerabilidade'
                 AND nivel_escolaridade IN ('Médio Completo', 'Superior')
            THEN 'Grupo de Baixo Risco Social'
            ELSE 'Grupo de Risco Intermediário'
        END AS categoria_risco_social
        
    FROM indicadores_base
    GROUP BY regiao, raca_cor, nivel_escolaridade, vulnerabilidade_social
),

-- Análise de variações geográficas
variacao_geografica AS (
    SELECT 
        estado,
        regiao,
        eh_capital,
        nivel_desenvolvimento,
        
        COUNT(*) AS total_nascimentos,
        
        -- Indicadores obstétricos
        AVG(CASE WHEN eh_cesareo THEN 1.0 ELSE 0.0 END) * 100 AS perc_cesareas,
        AVG(CASE WHEN eh_prematuro THEN 1.0 ELSE 0.0 END) * 100 AS perc_prematuros,
        AVG(CASE WHEN gestacao_alto_risco THEN 1.0 ELSE 0.0 END) * 100 AS perc_gestacao_risco,
        
        -- Indicadores de qualidade assistencial
        AVG(CASE WHEN prenatal_adequado THEN 1.0 ELSE 0.0 END) * 100 AS perc_prenatal_adequado,
        AVG(CASE WHEN apgar5 >= 7 THEN 1.0 ELSE 0.0 END) * 100 AS perc_apgar5_normal,
        
        -- Outcomes médios
        AVG(peso_nascimento) AS peso_medio,
        AVG(idade_mae) AS idade_mae_media,
        
        -- Desvios padrão para medir variabilidade
        STDDEV(peso_nascimento) AS variabilidade_peso,
        STDDEV(idade_mae) AS variabilidade_idade_mae,
        
        -- Ranking relativo
        RANK() OVER (ORDER BY AVG(CASE WHEN prenatal_adequado THEN 1.0 ELSE 0.0 END) DESC) AS rank_qualidade_prenatal,
        RANK() OVER (ORDER BY AVG(peso_nascimento) DESC) AS rank_peso_nascimento
        
    FROM indicadores_base
    GROUP BY estado, regiao, eh_capital, nivel_desenvolvimento
),

-- Análise temporal e sazonal
tendencias_temporais AS (
    SELECT 
        ano,
        mes,
        trimestre,
        estacao,
        
        COUNT(*) AS total_nascimentos,
        
        -- Indicadores mensais
        AVG(CASE WHEN eh_cesareo THEN 1.0 ELSE 0.0 END) * 100 AS perc_cesareas,
        AVG(CASE WHEN eh_prematuro THEN 1.0 ELSE 0.0 END) * 100 AS perc_prematuros,
        AVG(peso_nascimento) AS peso_medio,
        
        -- Análise de sazonalidade
        AVG(peso_nascimento) - LAG(AVG(peso_nascimento)) OVER (ORDER BY ano, mes) AS variacao_peso_mensal,
        
        -- Tendência de cesáreas
        AVG(CASE WHEN eh_cesareo THEN 1.0 ELSE 0.0 END) - 
        LAG(AVG(CASE WHEN eh_cesareo THEN 1.0 ELSE 0.0 END)) OVER (ORDER BY ano, mes) AS variacao_cesareas_mensal
        
    FROM indicadores_base
    GROUP BY ano, mes, trimestre, estacao
),

-- Análise de fatores de risco
fatores_risco AS (
    SELECT 
        gestacao_alto_risco,
        vulnerabilidade_social,
        complexidade_parto,
        
        COUNT(*) AS total_casos,
        
        -- Outcomes por grupo de risco
        AVG(CASE WHEN nivel_risco_neonatal = 'Alto Risco' THEN 1.0 ELSE 0.0 END) * 100 AS perc_alto_risco_neonatal,
        AVG(CASE WHEN apgar5 < 7 THEN 1.0 ELSE 0.0 END) * 100 AS perc_apgar5_baixo,
        AVG(CASE WHEN peso_nascimento < 2500 THEN 1.0 ELSE 0.0 END) * 100 AS perc_baixo_peso,
        AVG(CASE WHEN eh_prematuro THEN 1.0 ELSE 0.0 END) * 100 AS perc_prematuridade,
        
        -- Risco relativo (aproximado)
        AVG(CASE WHEN nivel_risco_neonatal = 'Alto Risco' THEN 1.0 ELSE 0.0 END) / 
        (SELECT AVG(CASE WHEN nivel_risco_neonatal = 'Alto Risco' THEN 1.0 ELSE 0.0 END) 
         FROM indicadores_base) AS risco_relativo_alto_risco,
         
        -- Média de peso estratificada
        AVG(peso_nascimento) AS peso_medio_grupo,
        
        -- Proporção no total
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM indicadores_base) AS perc_populacao
        
    FROM indicadores_base
    GROUP BY gestacao_alto_risco, vulnerabilidade_social, complexidade_parto
)

-- Consolidação final das análises epidemiológicas
SELECT 
    'DESIGUALDADES_SOCIAIS' AS tipo_analise,
    regiao AS dimensao_1,
    raca_cor AS dimensao_2,
    nivel_escolaridade AS dimensao_3,
    vulnerabilidade_social AS dimensao_4,
    categoria_risco_social AS dimensao_5,
    total_nascimentos,
    ROUND(taxa_prenatal_adequado * 100, 2) AS indicador_1,
    ROUND(taxa_cesarea * 100, 2) AS indicador_2,
    ROUND(taxa_prematuridade * 100, 2) AS indicador_3,
    ROUND(peso_medio, 1) AS indicador_4,
    ROUND(taxa_outcome_otimo * 100, 2) AS indicador_5,
    CURRENT_TIMESTAMP() AS data_analise
FROM desigualdades_sociais

UNION ALL

SELECT 
    'VARIACAO_GEOGRAFICA' AS tipo_analise,
    estado AS dimensao_1,
    regiao AS dimensao_2,
    CAST(eh_capital AS STRING) AS dimensao_3,
    nivel_desenvolvimento AS dimensao_4,
    NULL AS dimensao_5,
    total_nascimentos,
    ROUND(perc_cesareas, 2) AS indicador_1,
    ROUND(perc_prematuros, 2) AS indicador_2,
    ROUND(perc_prenatal_adequado, 2) AS indicador_3,
    ROUND(peso_medio, 1) AS indicador_4,
    CAST(rank_qualidade_prenatal AS FLOAT) AS indicador_5,
    CURRENT_TIMESTAMP() AS data_analise
FROM variacao_geografica

UNION ALL

SELECT 
    'TENDENCIAS_TEMPORAIS' AS tipo_analise,
    CAST(ano AS STRING) AS dimensao_1,
    CAST(mes AS STRING) AS dimensao_2,
    trimestre AS dimensao_3,
    estacao AS dimensao_4,
    NULL AS dimensao_5,
    total_nascimentos,
    ROUND(perc_cesareas, 2) AS indicador_1,
    ROUND(perc_prematuros, 2) AS indicador_2,
    ROUND(peso_medio, 1) AS indicador_3,
    ROUND(COALESCE(variacao_peso_mensal, 0), 2) AS indicador_4,
    ROUND(COALESCE(variacao_cesareas_mensal, 0), 4) AS indicador_5,
    CURRENT_TIMESTAMP() AS data_analise
FROM tendencias_temporais

UNION ALL

SELECT 
    'FATORES_RISCO' AS tipo_analise,
    CAST(gestacao_alto_risco AS STRING) AS dimensao_1,
    vulnerabilidade_social AS dimensao_2,
    complexidade_parto AS dimensao_3,
    NULL AS dimensao_4,
    NULL AS dimensao_5,
    total_casos AS total_nascimentos,
    ROUND(perc_alto_risco_neonatal, 2) AS indicador_1,
    ROUND(perc_baixo_peso, 2) AS indicador_2,
    ROUND(perc_prematuridade, 2) AS indicador_3,
    ROUND(peso_medio_grupo, 1) AS indicador_4,
    ROUND(risco_relativo_alto_risco, 3) AS indicador_5,
    CURRENT_TIMESTAMP() AS data_analise
FROM fatores_risco
