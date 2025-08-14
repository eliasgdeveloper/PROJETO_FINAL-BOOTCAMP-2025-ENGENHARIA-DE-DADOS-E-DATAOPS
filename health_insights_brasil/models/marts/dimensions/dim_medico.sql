-- Dimensão médica com características do parto e condições clínicas
{{ config(
    materialized='table',
    tags=['marts', 'dimensions']
) }}

WITH dados_medicos AS (
    SELECT DISTINCT
        tprobson AS grupo_robson,
        parto AS tipo_parto,
        consultas,
        gestacao AS semanas_gestacao,
        gravidez AS tipo_gravidez,
        semagestac AS classificacao_gestacional,
        tpapresent AS tipo_apresentacao,
        sttrabpart AS inicio_trabalho_parto,
        stcesparto AS cesarea_antes_trabalho,
        apgar1,
        apgar5,
        peso AS peso_nascimento,
        COALESCE(idanomal, '9') AS anomalia_congenita
    FROM {{ ref('stg_sinasc') }}
),

classificacoes_medicas AS (
    SELECT 
        *,
        
        -- Classificação do tipo de parto
        CASE 
            WHEN tipo_parto = '1' THEN 'Vaginal'
            WHEN tipo_parto = '2' THEN 'Cesáreo'
            ELSE 'Não Informado'
        END AS modalidade_parto,
        
        -- Classificação da idade gestacional
        CASE 
            WHEN semanas_gestacao < 22 THEN 'Aborto (<22 sem)'
            WHEN semanas_gestacao BETWEEN 22 AND 27 THEN 'Extremo Prematuro (22-27)'
            WHEN semanas_gestacao BETWEEN 28 AND 31 THEN 'Muito Prematuro (28-31)'
            WHEN semanas_gestacao BETWEEN 32 AND 36 THEN 'Prematuro Moderado (32-36)'
            WHEN semanas_gestacao BETWEEN 37 AND 41 THEN 'A Termo (37-41)'
            WHEN semanas_gestacao >= 42 THEN 'Pós-termo (42+)'
            ELSE 'Não Informado'
        END AS categoria_idade_gestacional,
        
        -- Classificação do peso ao nascer
        CASE 
            WHEN peso_nascimento < 1000 THEN 'Extremo Baixo Peso (<1000g)'
            WHEN peso_nascimento BETWEEN 1000 AND 1499 THEN 'Muito Baixo Peso (1000-1499g)'
            WHEN peso_nascimento BETWEEN 1500 AND 2499 THEN 'Baixo Peso (1500-2499g)'
            WHEN peso_nascimento BETWEEN 2500 AND 3999 THEN 'Peso Normal (2500-3999g)'
            WHEN peso_nascimento >= 4000 THEN 'Macrossomia (4000g+)'
            ELSE 'Não Informado'
        END AS categoria_peso,
        
        -- Classificação APGAR 1 minuto
        CASE 
            WHEN apgar1 BETWEEN 0 AND 3 THEN 'Grave (0-3)'
            WHEN apgar1 BETWEEN 4 AND 6 THEN 'Moderado (4-6)'
            WHEN apgar1 BETWEEN 7 AND 10 THEN 'Normal (7-10)'
            ELSE 'Não Avaliado'
        END AS categoria_apgar1,
        
        -- Classificação APGAR 5 minutos
        CASE 
            WHEN apgar5 BETWEEN 0 AND 3 THEN 'Grave (0-3)'
            WHEN apgar5 BETWEEN 4 AND 6 THEN 'Moderado (4-6)'
            WHEN apgar5 BETWEEN 7 AND 10 THEN 'Normal (7-10)'
            ELSE 'Não Avaliado'
        END AS categoria_apgar5,
        
        -- Tipo de gravidez
        CASE 
            WHEN tipo_gravidez = '1' THEN 'Única'
            WHEN tipo_gravidez = '2' THEN 'Dupla'
            WHEN tipo_gravidez = '3' THEN 'Tripla ou mais'
            ELSE 'Não Informado'
        END AS modalidade_gravidez,
        
        -- Adequação do pré-natal
        CASE 
            WHEN consultas = 0 THEN 'Nenhuma Consulta'
            WHEN consultas BETWEEN 1 AND 3 THEN 'Inadequado (1-3)'
            WHEN consultas BETWEEN 4 AND 6 THEN 'Parcial (4-6)'
            WHEN consultas >= 7 THEN 'Adequado (7+)'
            ELSE 'Não Informado'
        END AS adequacao_consultas,
        
        -- Grupo de Robson (classificação internacional)
        CASE 
            WHEN grupo_robson = '1' THEN 'Nulíparas termo cefálico trabalho espontâneo'
            WHEN grupo_robson = '2' THEN 'Nulíparas termo cefálico induzido/cesárea'
            WHEN grupo_robson = '3' THEN 'Multíparas termo cefálico trabalho espontâneo'
            WHEN grupo_robson = '4' THEN 'Multíparas termo cefálico induzido/cesárea'
            WHEN grupo_robson = '5' THEN 'Multíparas cesárea prévia termo cefálico'
            WHEN grupo_robson = '6' THEN 'Nulíparas pélvico'
            WHEN grupo_robson = '7' THEN 'Multíparas pélvico'
            WHEN grupo_robson = '8' THEN 'Múltiplas gestações'
            WHEN grupo_robson = '9' THEN 'Situação anômala'
            WHEN grupo_robson = '10' THEN 'Prematuros cefálicos'
            ELSE 'Não Classificado'
        END AS descricao_robson,
        
        -- Início do trabalho de parto
        CASE 
            WHEN inicio_trabalho_parto = '1' THEN 'Espontâneo'
            WHEN inicio_trabalho_parto = '2' THEN 'Induzido'
            WHEN inicio_trabalho_parto = '3' THEN 'Não se aplica'
            ELSE 'Não Informado'
        END AS modalidade_inicio_parto,
        
        -- Presença de anomalias
        CASE 
            WHEN anomalia_congenita = '1' THEN 'Sim'
            WHEN anomalia_congenita = '2' THEN 'Não'
            ELSE 'Não Informado'
        END AS presenca_anomalia
        
    FROM dados_medicos
),

indicadores_risco AS (
    SELECT 
        *,
        
        -- Risco neonatal baseado em múltiplos fatores
        CASE 
            WHEN categoria_peso IN ('Extremo Baixo Peso (<1000g)', 'Muito Baixo Peso (1000-1499g)')
                 OR categoria_idade_gestacional IN ('Extremo Prematuro (22-27)', 'Muito Prematuro (28-31)')
                 OR categoria_apgar5 = 'Grave (0-3)' THEN 'Alto Risco'
            WHEN categoria_peso = 'Baixo Peso (1500-2499g)'
                 OR categoria_idade_gestacional = 'Prematuro Moderado (32-36)'
                 OR categoria_apgar5 = 'Moderado (4-6)' THEN 'Médio Risco'
            WHEN categoria_peso = 'Peso Normal (2500-3999g)'
                 AND categoria_idade_gestacional = 'A Termo (37-41)'
                 AND categoria_apgar5 = 'Normal (7-10)' THEN 'Baixo Risco'
            ELSE 'Risco Indeterminado'
        END AS nivel_risco_neonatal,
        
        -- Complexidade do parto
        CASE 
            WHEN modalidade_parto = 'Cesáreo' 
                 AND modalidade_gravidez != 'Única'
                 AND categoria_idade_gestacional IN ('Extremo Prematuro (22-27)', 'Muito Prematuro (28-31)') 
            THEN 'Alta Complexidade'
            WHEN modalidade_parto = 'Cesáreo' 
                 OR modalidade_gravidez != 'Única'
                 OR categoria_idade_gestacional NOT IN ('A Termo (37-41)') 
            THEN 'Média Complexidade'
            ELSE 'Baixa Complexidade'
        END AS complexidade_parto,
        
        -- Indicador de qualidade assistencial
        CASE 
            WHEN adequacao_consultas = 'Adequado (7+)'
                 AND categoria_apgar5 = 'Normal (7-10)'
                 AND categoria_peso IN ('Peso Normal (2500-3999g)')
            THEN 'Excelente'
            WHEN adequacao_consultas IN ('Adequado (7+)', 'Parcial (4-6)')
                 AND categoria_apgar5 IN ('Normal (7-10)', 'Moderado (4-6)')
            THEN 'Boa'
            ELSE 'Necessita Melhoria'
        END AS qualidade_assistencial,
        
        -- Perfil da gestação
        CONCAT(
            modalidade_gravidez, ' - ',
            categoria_idade_gestacional, ' - ',
            modalidade_parto
        ) AS perfil_gestacao_completo
        
    FROM classificacoes_medicas
)

SELECT 
    -- Chave surrogate
    ROW_NUMBER() OVER (ORDER BY grupo_robson, tipo_parto, semanas_gestacao, peso_nascimento) AS sk_medico,
    
    -- Dados originais
    grupo_robson,
    tipo_parto,
    consultas,
    semanas_gestacao,
    tipo_gravidez,
    classificacao_gestacional,
    tipo_apresentacao,
    inicio_trabalho_parto,
    cesarea_antes_trabalho,
    apgar1,
    apgar5,
    peso_nascimento,
    anomalia_congenita,
    
    -- Classificações derivadas
    modalidade_parto,
    categoria_idade_gestacional,
    categoria_peso,
    categoria_apgar1,
    categoria_apgar5,
    modalidade_gravidez,
    adequacao_consultas,
    descricao_robson,
    modalidade_inicio_parto,
    presenca_anomalia,
    
    -- Indicadores de risco e qualidade
    nivel_risco_neonatal,
    complexidade_parto,
    qualidade_assistencial,
    perfil_gestacao_completo,
    
    -- Flags para análises
    CASE WHEN modalidade_parto = 'Cesáreo' THEN TRUE ELSE FALSE END AS eh_cesareo,
    CASE WHEN categoria_idade_gestacional LIKE '%Prematuro%' THEN TRUE ELSE FALSE END AS eh_prematuro,
    CASE WHEN categoria_peso LIKE '%Baixo Peso%' THEN TRUE ELSE FALSE END AS eh_baixo_peso,
    CASE WHEN categoria_apgar5 = 'Normal (7-10)' THEN TRUE ELSE FALSE END AS apgar5_normal,
    CASE WHEN adequacao_consultas = 'Adequado (7+)' THEN TRUE ELSE FALSE END AS prenatal_adequado,
    CASE WHEN presenca_anomalia = 'Sim' THEN TRUE ELSE FALSE END AS tem_anomalia,
    CASE WHEN modalidade_gravidez != 'Única' THEN TRUE ELSE FALSE END AS gestacao_multipla,
    
    -- Metadados
    CURRENT_TIMESTAMP() AS data_criacao,
    'ELIAS' AS criado_por
    
FROM indicadores_risco
ORDER BY grupo_robson, categoria_idade_gestacional, categoria_peso
