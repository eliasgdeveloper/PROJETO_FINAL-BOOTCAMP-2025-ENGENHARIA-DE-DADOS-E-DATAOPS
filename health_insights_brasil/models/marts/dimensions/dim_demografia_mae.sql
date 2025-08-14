-- Dimensão demográfica com análise detalhada das características da mãe
{{ config(
    materialized='table',
    tags=['marts', 'dimensions']
) }}

WITH dados_demograficos AS (
    SELECT DISTINCT
        idade_mae,
        escmae AS escolaridade_mae,
        estcivmae AS estado_civil_mae,
        racacormae AS raca_cor_mae,
        qtdgestant AS qtd_gestacoes,
        qtdpartnor AS qtd_partos_normais,
        qtdpartces AS qtd_partos_cesareos,
        COALESCE(consultas, 0) AS num_consultas_prenatal
    FROM {{ ref('stg_sinasc') }}
    WHERE idade_mae IS NOT NULL
),

classificacoes_demograficas AS (
    SELECT 
        *,
        
        -- Classificação etária detalhada
        CASE 
            WHEN idade_mae < 15 THEN 'Muito Jovem (<15)'
            WHEN idade_mae BETWEEN 15 AND 19 THEN 'Adolescente (15-19)'
            WHEN idade_mae BETWEEN 20 AND 24 THEN 'Jovem Adulta (20-24)'
            WHEN idade_mae BETWEEN 25 AND 29 THEN 'Adulta Jovem (25-29)'
            WHEN idade_mae BETWEEN 30 AND 34 THEN 'Adulta (30-34)'
            WHEN idade_mae BETWEEN 35 AND 39 THEN 'Adulta Madura (35-39)'
            WHEN idade_mae BETWEEN 40 AND 44 THEN 'Madura (40-44)'
            WHEN idade_mae >= 45 THEN 'Idade Avançada (45+)'
            ELSE 'Não Classificado'
        END AS faixa_etaria_detalhada,
        
        -- Classificação de risco obstétrico por idade
        CASE 
            WHEN idade_mae < 18 OR idade_mae > 35 THEN 'Alto Risco'
            WHEN idade_mae BETWEEN 18 AND 35 THEN 'Baixo Risco'
            ELSE 'Indefinido'
        END AS risco_obstetrico_idade,
        
        -- Escolaridade padronizada
        CASE 
            WHEN escolaridade_mae IN ('1', '2') THEN 'Fundamental Incompleto'
            WHEN escolaridade_mae = '3' THEN 'Fundamental Completo'
            WHEN escolaridade_mae = '4' THEN 'Médio Incompleto'
            WHEN escolaridade_mae = '5' THEN 'Médio Completo'
            WHEN escolaridade_mae IN ('6', '7', '8') THEN 'Superior'
            ELSE 'Não Informado'
        END AS nivel_escolaridade,
        
        -- Estado civil padronizado
        CASE 
            WHEN estado_civil_mae = '1' THEN 'Solteira'
            WHEN estado_civil_mae = '2' THEN 'Casada'
            WHEN estado_civil_mae = '3' THEN 'Viúva'
            WHEN estado_civil_mae = '4' THEN 'Separada/Divorciada'
            WHEN estado_civil_mae = '5' THEN 'União Estável'
            ELSE 'Não Informado'
        END AS situacao_civil,
        
        -- Raça/cor padronizada
        CASE 
            WHEN raca_cor_mae = '1' THEN 'Branca'
            WHEN raca_cor_mae = '2' THEN 'Preta'
            WHEN raca_cor_mae = '3' THEN 'Amarela'
            WHEN raca_cor_mae = '4' THEN 'Parda'
            WHEN raca_cor_mae = '5' THEN 'Indígena'
            ELSE 'Não Informado'
        END AS raca_cor,
        
        -- Análise de experiência reprodutiva
        COALESCE(qtd_gestacoes, 0) + COALESCE(qtd_partos_normais, 0) + COALESCE(qtd_partos_cesareos, 0) AS experiencia_reprodutiva_total,
        
        CASE 
            WHEN COALESCE(qtd_gestacoes, 0) = 0 THEN 'Primigesta'
            WHEN COALESCE(qtd_gestacoes, 0) BETWEEN 1 AND 3 THEN 'Multigesta'
            WHEN COALESCE(qtd_gestacoes, 0) > 3 THEN 'Grande Multigesta'
            ELSE 'Não Informado'
        END AS classificacao_gestacional,
        
        -- Análise do pré-natal
        CASE 
            WHEN num_consultas_prenatal = 0 THEN 'Sem Pré-natal'
            WHEN num_consultas_prenatal BETWEEN 1 AND 3 THEN 'Inadequado (1-3)'
            WHEN num_consultas_prenatal BETWEEN 4 AND 6 THEN 'Parcial (4-6)'
            WHEN num_consultas_prenatal >= 7 THEN 'Adequado (7+)'
            ELSE 'Não Informado'
        END AS adequacao_prenatal,
        
        -- Preferência por tipo de parto
        CASE 
            WHEN COALESCE(qtd_partos_cesareos, 0) > COALESCE(qtd_partos_normais, 0) THEN 'Histórico Cesárea'
            WHEN COALESCE(qtd_partos_normais, 0) > COALESCE(qtd_partos_cesareos, 0) THEN 'Histórico Normal'
            WHEN COALESCE(qtd_partos_normais, 0) = COALESCE(qtd_partos_cesareos, 0) AND 
                 COALESCE(qtd_partos_normais, 0) > 0 THEN 'Histórico Misto'
            ELSE 'Primeiro Parto'
        END AS perfil_parto_historico
        
    FROM dados_demograficos
),

indicadores_compostos AS (
    SELECT 
        *,
        
        -- Índice de vulnerabilidade social (simplificado)
        CASE 
            WHEN nivel_escolaridade IN ('Fundamental Incompleto', 'Não Informado') 
                 AND situacao_civil IN ('Solteira', 'Não Informado')
                 AND idade_mae < 20 THEN 'Alta Vulnerabilidade'
            WHEN nivel_escolaridade = 'Médio Completo' 
                 AND situacao_civil IN ('Casada', 'União Estável')
                 AND idade_mae BETWEEN 20 AND 35 THEN 'Baixa Vulnerabilidade'
            ELSE 'Média Vulnerabilidade'
        END AS vulnerabilidade_social,
        
        -- Perfil de cuidado materno
        CASE 
            WHEN adequacao_prenatal = 'Adequado (7+)' 
                 AND nivel_escolaridade IN ('Médio Completo', 'Superior') THEN 'Alto Cuidado'
            WHEN adequacao_prenatal IN ('Parcial (4-6)', 'Adequado (7+)')
                 AND nivel_escolaridade != 'Não Informado' THEN 'Médio Cuidado'
            ELSE 'Baixo Cuidado'
        END AS perfil_cuidado_materno,
        
        -- Segmento demográfico
        CONCAT(
            faixa_etaria_detalhada, ' - ',
            nivel_escolaridade, ' - ',
            raca_cor
        ) AS segmento_demografico_completo
        
    FROM classificacoes_demograficas
)

SELECT 
    -- Chave surrogate
    ROW_NUMBER() OVER (ORDER BY idade_mae, escolaridade_mae, estado_civil_mae, raca_cor_mae) AS sk_demografia,
    
    -- Dados originais
    idade_mae,
    escolaridade_mae,
    estado_civil_mae,
    raca_cor_mae,
    qtd_gestacoes,
    qtd_partos_normais,
    qtd_partos_cesareos,
    num_consultas_prenatal,
    
    -- Classificações derivadas
    faixa_etaria_detalhada,
    risco_obstetrico_idade,
    nivel_escolaridade,
    situacao_civil,
    raca_cor,
    classificacao_gestacional,
    adequacao_prenatal,
    perfil_parto_historico,
    
    -- Indicadores compostos
    experiencia_reprodutiva_total,
    vulnerabilidade_social,
    perfil_cuidado_materno,
    segmento_demografico_completo,
    
    -- Flags para análises
    CASE WHEN idade_mae < 18 OR idade_mae > 35 THEN TRUE ELSE FALSE END AS gestacao_alto_risco,
    CASE WHEN num_consultas_prenatal >= 7 THEN TRUE ELSE FALSE END AS prenatal_adequado,
    CASE WHEN nivel_escolaridade IN ('Médio Completo', 'Superior') THEN TRUE ELSE FALSE END AS escolaridade_alta,
    CASE WHEN situacao_civil IN ('Casada', 'União Estável') THEN TRUE ELSE FALSE END AS situacao_estavel,
    
    -- Metadados
    CURRENT_TIMESTAMP() AS data_criacao,
    'ELIAS' AS criado_por
    
FROM indicadores_compostos
ORDER BY idade_mae, nivel_escolaridade, situacao_civil
