-- Modelo de agregações de nascimentos para análises de alta performance
{{ config(
    materialized='table',
    tags=['marts', 'agregacoes']
) }}

WITH nascimentos_base AS (
    SELECT 
        f.data_nascimento,
        f.sk_geografia,
        f.sk_demografia,
        f.sk_medico,
        f.peso_nascimento,
        f.apgar5,
        f.semanas_gestacao,
        g.estado,
        g.regiao,
        g.eh_capital,
        d.idade_mae,
        d.faixa_etaria_detalhada,
        d.nivel_escolaridade,
        d.vulnerabilidade_social,
        m.modalidade_parto,
        m.categoria_idade_gestacional,
        m.categoria_peso,
        m.nivel_risco_neonatal,
        m.eh_cesareo,
        m.eh_prematuro,
        m.prenatal_adequado
    FROM {{ ref('fct_nascimentos') }} f
    JOIN {{ ref('dim_geografia') }} g ON f.sk_geografia = g.sk_geografia
    JOIN {{ ref('dim_demografia_mae') }} d ON f.sk_demografia = d.sk_demografia
    JOIN {{ ref('dim_medico') }} m ON f.sk_medico = m.sk_medico
),

-- Agregações por tempo
agregacoes_temporais AS (
    SELECT 
        DATE_TRUNC('MONTH', data_nascimento) AS mes_ano,
        EXTRACT(YEAR FROM data_nascimento) AS ano,
        EXTRACT(MONTH FROM data_nascimento) AS mes,
        EXTRACT(QUARTER FROM data_nascimento) AS trimestre,
        
        -- Contadores básicos
        COUNT(*) AS total_nascimentos,
        COUNT(CASE WHEN eh_cesareo THEN 1 END) AS total_cesareas,
        COUNT(CASE WHEN eh_prematuro THEN 1 END) AS total_prematuros,
        COUNT(CASE WHEN prenatal_adequado THEN 1 END) AS total_prenatal_adequado,
        
        -- Médias e indicadores
        AVG(peso_nascimento) AS peso_medio,
        AVG(apgar5) AS apgar5_medio,
        AVG(semanas_gestacao) AS semanas_gestacao_media,
        AVG(idade_mae) AS idade_mae_media,
        
        -- Percentuais
        ROUND(COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_cesareas,
        ROUND(COUNT(CASE WHEN eh_prematuro THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_prematuros,
        ROUND(COUNT(CASE WHEN prenatal_adequado THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_prenatal_adequado,
        
        -- Indicadores de qualidade
        ROUND(COUNT(CASE WHEN apgar5 >= 7 THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_apgar5_normal,
        ROUND(COUNT(CASE WHEN peso_nascimento >= 2500 THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_peso_adequado
        
    FROM nascimentos_base
    GROUP BY DATE_TRUNC('MONTH', data_nascimento), ano, mes, trimestre
),

-- Agregações por geografia
agregacoes_geograficas AS (
    SELECT 
        estado,
        regiao,
        eh_capital,
        
        -- Contadores básicos
        COUNT(*) AS total_nascimentos,
        COUNT(CASE WHEN eh_cesareo THEN 1 END) AS total_cesareas,
        COUNT(CASE WHEN eh_prematuro THEN 1 END) AS total_prematuros,
        COUNT(CASE WHEN vulnerabilidade_social = 'Alta Vulnerabilidade' THEN 1 END) AS total_alta_vulnerabilidade,
        
        -- Médias por estado
        AVG(peso_nascimento) AS peso_medio,
        AVG(apgar5) AS apgar5_medio,
        AVG(idade_mae) AS idade_mae_media,
        
        -- Percentuais por estado
        ROUND(COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_cesareas,
        ROUND(COUNT(CASE WHEN eh_prematuro THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_prematuros,
        ROUND(COUNT(CASE WHEN vulnerabilidade_social = 'Alta Vulnerabilidade' THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_alta_vulnerabilidade,
        
        -- Ranking
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_nascimentos,
        RANK() OVER (ORDER BY AVG(peso_nascimento) DESC) AS rank_peso_medio,
        RANK() OVER (ORDER BY COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*)) AS rank_cesareas
        
    FROM nascimentos_base
    GROUP BY estado, regiao, eh_capital
),

-- Agregações por perfil demográfico
agregacoes_demograficas AS (
    SELECT 
        faixa_etaria_detalhada,
        nivel_escolaridade,
        vulnerabilidade_social,
        
        -- Contadores
        COUNT(*) AS total_nascimentos,
        COUNT(CASE WHEN eh_cesareo THEN 1 END) AS total_cesareas,
        COUNT(CASE WHEN nivel_risco_neonatal = 'Alto Risco' THEN 1 END) AS total_alto_risco,
        
        -- Médias
        AVG(peso_nascimento) AS peso_medio,
        AVG(apgar5) AS apgar5_medio,
        
        -- Percentuais
        ROUND(COUNT(CASE WHEN eh_cesareo THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_cesareas,
        ROUND(COUNT(CASE WHEN nivel_risco_neonatal = 'Alto Risco' THEN 1 END) * 100.0 / COUNT(*), 2) AS perc_alto_risco
        
    FROM nascimentos_base
    GROUP BY faixa_etaria_detalhada, nivel_escolaridade, vulnerabilidade_social
),

-- Agregações por características médicas
agregacoes_medicas AS (
    SELECT 
        modalidade_parto,
        categoria_idade_gestacional,
        categoria_peso,
        nivel_risco_neonatal,
        
        -- Contadores
        COUNT(*) AS total_nascimentos,
        COUNT(CASE WHEN apgar5 >= 7 THEN 1 END) AS total_apgar5_normal,
        
        -- Médias específicas
        AVG(peso_nascimento) AS peso_medio,
        AVG(apgar5) AS apgar5_medio,
        AVG(semanas_gestacao) AS semanas_gestacao_media,
        
        -- Desvios padrão para análise de variabilidade
        STDDEV(peso_nascimento) AS desvio_peso,
        STDDEV(apgar5) AS desvio_apgar5
        
    FROM nascimentos_base
    GROUP BY modalidade_parto, categoria_idade_gestacional, categoria_peso, nivel_risco_neonatal
)

-- Combinação final das agregações em uma estrutura unificada
SELECT 
    'TEMPORAL' AS tipo_agregacao,
    CAST(ano AS STRING) AS dimensao_1,
    CAST(mes AS STRING) AS dimensao_2,
    CAST(trimestre AS STRING) AS dimensao_3,
    NULL AS dimensao_4,
    total_nascimentos,
    total_cesareas,
    total_prematuros,
    peso_medio,
    apgar5_medio,
    perc_cesareas,
    perc_prematuros,
    NULL AS rank_nascimentos,
    CURRENT_TIMESTAMP() AS data_criacao
FROM agregacoes_temporais

UNION ALL

SELECT 
    'GEOGRAFICA' AS tipo_agregacao,
    estado AS dimensao_1,
    regiao AS dimensao_2,
    CAST(eh_capital AS STRING) AS dimensao_3,
    NULL AS dimensao_4,
    total_nascimentos,
    total_cesareas,
    total_prematuros,
    peso_medio,
    apgar5_medio,
    perc_cesareas,
    perc_prematuros,
    rank_nascimentos,
    CURRENT_TIMESTAMP() AS data_criacao
FROM agregacoes_geograficas

UNION ALL

SELECT 
    'DEMOGRAFICA' AS tipo_agregacao,
    faixa_etaria_detalhada AS dimensao_1,
    nivel_escolaridade AS dimensao_2,
    vulnerabilidade_social AS dimensao_3,
    NULL AS dimensao_4,
    total_nascimentos,
    total_cesareas,
    total_alto_risco AS total_prematuros,
    peso_medio,
    apgar5_medio,
    perc_cesareas,
    perc_alto_risco AS perc_prematuros,
    NULL AS rank_nascimentos,
    CURRENT_TIMESTAMP() AS data_criacao
FROM agregacoes_demograficas

UNION ALL

SELECT 
    'MEDICA' AS tipo_agregacao,
    modalidade_parto AS dimensao_1,
    categoria_idade_gestacional AS dimensao_2,
    categoria_peso AS dimensao_3,
    nivel_risco_neonatal AS dimensao_4,
    total_nascimentos,
    NULL AS total_cesareas,
    NULL AS total_prematuros,
    peso_medio,
    apgar5_medio,
    NULL AS perc_cesareas,
    NULL AS perc_prematuros,
    NULL AS rank_nascimentos,
    CURRENT_TIMESTAMP() AS data_criacao
FROM agregacoes_medicas
