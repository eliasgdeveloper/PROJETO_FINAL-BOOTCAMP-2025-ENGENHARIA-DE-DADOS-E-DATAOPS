-- Testes de qualidade de dados e validações
{{ config(
    materialized='view',
    tags=['tests', 'qualidade']
) }}

WITH testes_basicos AS (
    -- Teste 1: Registros duplicados
    SELECT 
        'Duplicados na Staging' AS teste,
        'CRITICO' AS severidade,
        COUNT(*) - COUNT(DISTINCT CONCAT(
            COALESCE(data_nascimento, ''), 
            COALESCE(codmunres, ''), 
            COALESCE(idade_mae, ''),
            COALESCE(peso, '')
        )) AS falhas_detectadas,
        CASE 
            WHEN COUNT(*) - COUNT(DISTINCT CONCAT(
                COALESCE(data_nascimento, ''), 
                COALESCE(codmunres, ''), 
                COALESCE(idade_mae, ''),
                COALESCE(peso, '')
            )) = 0 THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Verificar duplicatas nos dados source' AS acao_recomendada
    FROM {{ ref('stg_sinasc') }}
    
    UNION ALL
    
    -- Teste 2: Valores nulos em campos críticos
    SELECT 
        'Nulos em Campos Críticos' AS teste,
        'ALTO' AS severidade,
        COUNT(*) - COUNT(data_nascimento) - COUNT(codmunres) - COUNT(idade_mae) AS falhas_detectadas,
        CASE 
            WHEN (COUNT(*) - COUNT(data_nascimento) - COUNT(codmunres) - COUNT(idade_mae)) < COUNT(*) * 0.05 THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Investigar qualidade dos dados source' AS acao_recomendada
    FROM {{ ref('stg_sinasc') }}
    
    UNION ALL
    
    -- Teste 3: Integridade referencial fato-dimensões
    SELECT 
        'Integridade Referencial' AS teste,
        'CRITICO' AS severidade,
        (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }}) - 
        (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }} f
         JOIN {{ ref('dim_geografia') }} g ON f.sk_geografia = g.sk_geografia
         JOIN {{ ref('dim_demografia_mae') }} d ON f.sk_demografia = d.sk_demografia
         JOIN {{ ref('dim_medico') }} m ON f.sk_medico = m.sk_medico) AS falhas_detectadas,
        CASE 
            WHEN (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }}) = 
                 (SELECT COUNT(*) FROM {{ ref('fct_nascimentos') }} f
                  JOIN {{ ref('dim_geografia') }} g ON f.sk_geografia = g.sk_geografia
                  JOIN {{ ref('dim_demografia_mae') }} d ON f.sk_demografia = d.sk_demografia
                  JOIN {{ ref('dim_medico') }} m ON f.sk_medico = m.sk_medico) THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Verificar processo de criação das dimensões' AS acao_recomendada
),

testes_regras_negocio AS (
    -- Teste 4: Idades maternas válidas
    SELECT 
        'Idades Maternas Válidas' AS teste,
        'MEDIO' AS severidade,
        COUNT(CASE WHEN idade_mae < 10 OR idade_mae > 60 THEN 1 END) AS falhas_detectadas,
        CASE 
            WHEN COUNT(CASE WHEN idade_mae < 10 OR idade_mae > 60 THEN 1 END) < COUNT(*) * 0.01 THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Aplicar regras de limpeza para idades extremas' AS acao_recomendada
    FROM {{ ref('stg_sinasc') }}
    
    UNION ALL
    
    -- Teste 5: Peso ao nascer válido
    SELECT 
        'Peso Nascimento Válido' AS teste,
        'ALTO' AS severidade,
        COUNT(CASE WHEN peso < 300 OR peso > 6000 THEN 1 END) AS falhas_detectadas,
        CASE 
            WHEN COUNT(CASE WHEN peso < 300 OR peso > 6000 THEN 1 END) < COUNT(*) * 0.005 THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Revisar valores extremos de peso' AS acao_recomendada
    FROM {{ ref('stg_sinasc') }}
    WHERE peso IS NOT NULL
    
    UNION ALL
    
    -- Teste 6: APGAR 5min válido
    SELECT 
        'APGAR 5min Válido' AS teste,
        'MEDIO' AS severidade,
        COUNT(CASE WHEN apgar5 < 0 OR apgar5 > 10 THEN 1 END) AS falhas_detectadas,
        CASE 
            WHEN COUNT(CASE WHEN apgar5 < 0 OR apgar5 > 10 THEN 1 END) = 0 THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Validar escala APGAR nos dados source' AS acao_recomendada
    FROM {{ ref('stg_sinasc') }}
    WHERE apgar5 IS NOT NULL
    
    UNION ALL
    
    -- Teste 7: Idade gestacional válida
    SELECT 
        'Idade Gestacional Válida' AS teste,
        'ALTO' AS severidade,
        COUNT(CASE WHEN gestacao < 20 OR gestacao > 45 THEN 1 END) AS falhas_detectadas,
        CASE 
            WHEN COUNT(CASE WHEN gestacao < 20 OR gestacao > 45 THEN 1 END) < COUNT(*) * 0.01 THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Investigar registros com IG extrema' AS acao_recomendada
    FROM {{ ref('stg_sinasc') }}
    WHERE gestacao IS NOT NULL
),

testes_consistencia AS (
    -- Teste 8: Consistência temporal
    SELECT 
        'Consistência Temporal' AS teste,
        'MEDIO' AS severidade,
        COUNT(CASE WHEN data_nascimento > CURRENT_DATE() OR 
                        data_nascimento < DATE('2020-01-01') THEN 1 END) AS falhas_detectadas,
        CASE 
            WHEN COUNT(CASE WHEN data_nascimento > CURRENT_DATE() OR 
                             data_nascimento < DATE('2020-01-01') THEN 1 END) = 0 THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Verificar filtros de período nos dados' AS acao_recomendada
    FROM {{ ref('stg_sinasc') }}
    
    UNION ALL
    
    -- Teste 9: Cobertura geográfica
    SELECT 
        'Cobertura Geográfica' AS teste,
        'BAIXO' AS severidade,
        27 - COUNT(DISTINCT uf) AS falhas_detectadas,  -- 26 estados + DF
        CASE 
            WHEN COUNT(DISTINCT uf) >= 20 THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Verificar representatividade geográfica' AS acao_recomendada
    FROM {{ ref('stg_sinasc') }}
    
    UNION ALL
    
    -- Teste 10: Distribuição de partos
    SELECT 
        'Distribuição Tipos Parto' AS teste,
        'BAIXO' AS severidade,
        CASE 
            WHEN COUNT(CASE WHEN parto = '1' THEN 1 END) = 0 OR
                 COUNT(CASE WHEN parto = '2' THEN 1 END) = 0 THEN 1
            ELSE 0
        END AS falhas_detectadas,
        CASE 
            WHEN COUNT(CASE WHEN parto = '1' THEN 1 END) > 0 AND
                 COUNT(CASE WHEN parto = '2' THEN 1 END) > 0 THEN 'PASSOU'
            ELSE 'FALHOU'
        END AS resultado,
        'Verificar codificação dos tipos de parto' AS acao_recomendada
    FROM {{ ref('stg_sinasc') }}
    WHERE parto IS NOT NULL
),

resumo_testes AS (
    SELECT 
        COUNT(*) AS total_testes,
        COUNT(CASE WHEN resultado = 'PASSOU' THEN 1 END) AS testes_aprovados,
        COUNT(CASE WHEN resultado = 'FALHOU' THEN 1 END) AS testes_reprovados,
        ROUND(COUNT(CASE WHEN resultado = 'PASSOU' THEN 1 END) * 100.0 / COUNT(*), 1) AS taxa_aprovacao,
        
        -- Severidade das falhas
        COUNT(CASE WHEN resultado = 'FALHOU' AND severidade = 'CRITICO' THEN 1 END) AS falhas_criticas,
        COUNT(CASE WHEN resultado = 'FALHOU' AND severidade = 'ALTO' THEN 1 END) AS falhas_altas,
        COUNT(CASE WHEN resultado = 'FALHOU' AND severidade = 'MEDIO' THEN 1 END) AS falhas_medias,
        COUNT(CASE WHEN resultado = 'FALHOU' AND severidade = 'BAIXO' THEN 1 END) AS falhas_baixas,
        
        -- Status geral
        CASE 
            WHEN COUNT(CASE WHEN resultado = 'FALHOU' AND severidade = 'CRITICO' THEN 1 END) > 0 THEN 'CRÍTICO'
            WHEN COUNT(CASE WHEN resultado = 'FALHOU' AND severidade = 'ALTO' THEN 1 END) > 0 THEN 'ATENÇÃO'
            WHEN COUNT(CASE WHEN resultado = 'FALHOU' THEN 1 END) > 0 THEN 'ALERTA'
            ELSE 'SAUDÁVEL'
        END AS status_pipeline
        
    FROM (
        SELECT * FROM testes_basicos
        UNION ALL
        SELECT * FROM testes_regras_negocio  
        UNION ALL
        SELECT * FROM testes_consistencia
    )
)

-- Resultado final dos testes
SELECT 
    'RESULTADO_INDIVIDUAL' AS tipo_resultado,
    teste,
    severidade,
    resultado,
    falhas_detectadas,
    acao_recomendada,
    CURRENT_TIMESTAMP() AS executado_em,
    'ELIAS' AS executado_por
FROM (
    SELECT * FROM testes_basicos
    UNION ALL
    SELECT * FROM testes_regras_negocio  
    UNION ALL
    SELECT * FROM testes_consistencia
)

UNION ALL

SELECT 
    'RESUMO_GERAL' AS tipo_resultado,
    'Resumo da Execução' AS teste,
    status_pipeline AS severidade,
    CONCAT(testes_aprovados, '/', total_testes, ' testes aprovados') AS resultado,
    testes_reprovados AS falhas_detectadas,
    CASE 
        WHEN status_pipeline = 'CRÍTICO' THEN 'Interromper pipeline - falhas críticas detectadas'
        WHEN status_pipeline = 'ATENÇÃO' THEN 'Revisar dados antes de prosseguir'
        WHEN status_pipeline = 'ALERTA' THEN 'Monitorar qualidade dos dados'
        ELSE 'Pipeline saudável - prosseguir'
    END AS acao_recomendada,
    CURRENT_TIMESTAMP() AS executado_em,
    'ELIAS' AS executado_por
FROM resumo_testes
