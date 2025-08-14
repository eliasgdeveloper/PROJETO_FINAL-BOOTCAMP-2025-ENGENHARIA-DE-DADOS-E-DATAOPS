-- Dimensão temporal com hierarquias e sazonalidade
{{ config(
    materialized='table',
    tags=['marts', 'dimensions']
) }}

WITH datas_base AS (
    SELECT DISTINCT data_nascimento
    FROM {{ ref('stg_sinasc') }}
    WHERE data_nascimento IS NOT NULL
),

calendario_completo AS (
    SELECT 
        data_nascimento,
        
        -- Componentes temporais
        EXTRACT(YEAR FROM data_nascimento) AS ano,
        EXTRACT(MONTH FROM data_nascimento) AS mes,
        EXTRACT(DAY FROM data_nascimento) AS dia,
        EXTRACT(QUARTER FROM data_nascimento) AS trimestre,
        EXTRACT(WEEK FROM data_nascimento) AS semana_ano,
        EXTRACT(DAYOFWEEK FROM data_nascimento) AS dia_semana,
        EXTRACT(DAYOFYEAR FROM data_nascimento) AS dia_ano,
        
        -- Nomes formatados
        CASE EXTRACT(MONTH FROM data_nascimento)
            WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
            WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
            WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
            WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
        END AS nome_mes,
        
        CASE EXTRACT(DAYOFWEEK FROM data_nascimento)
            WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Segunda' WHEN 3 THEN 'Terça'
            WHEN 4 THEN 'Quarta' WHEN 5 THEN 'Quinta' WHEN 6 THEN 'Sexta' WHEN 7 THEN 'Sábado'
        END AS nome_dia_semana,
        
        'Q' || EXTRACT(QUARTER FROM data_nascimento) AS nome_trimestre,
        
        -- Classificações sazonais
        CASE 
            WHEN EXTRACT(MONTH FROM data_nascimento) IN (12, 1, 2) THEN 'Verão'
            WHEN EXTRACT(MONTH FROM data_nascimento) IN (3, 4, 5) THEN 'Outono'  
            WHEN EXTRACT(MONTH FROM data_nascimento) IN (6, 7, 8) THEN 'Inverno'
            WHEN EXTRACT(MONTH FROM data_nascimento) IN (9, 10, 11) THEN 'Primavera'
        END AS estacao,
        
        -- Períodos especiais
        CASE 
            WHEN EXTRACT(MONTH FROM data_nascimento) IN (12, 1, 2) THEN 'Férias de Verão'
            WHEN EXTRACT(MONTH FROM data_nascimento) IN (6, 7) THEN 'Férias de Inverno'
            ELSE 'Período Letivo'
        END AS periodo_escolar,
        
        -- Flags úteis
        CASE WHEN EXTRACT(DAYOFWEEK FROM data_nascimento) IN (1, 7) THEN TRUE ELSE FALSE END AS fim_de_semana,
        CASE WHEN EXTRACT(DAY FROM data_nascimento) <= 15 THEN 'Primeira Quinzena' ELSE 'Segunda Quinzena' END AS quinzena,
        
        -- Datas derivadas
        DATE_TRUNC('MONTH', data_nascimento) AS primeiro_dia_mes,
        LAST_DAY(data_nascimento) AS ultimo_dia_mes,
        DATE_TRUNC('YEAR', data_nascimento) AS primeiro_dia_ano,
        
        -- Indicadores de análise temporal
        LAG(data_nascimento) OVER (ORDER BY data_nascimento) AS data_anterior,
        LEAD(data_nascimento) OVER (ORDER BY data_nascimento) AS proxima_data
        
    FROM datas_base
)

SELECT 
    *,
    -- Chave surrogate
    ROW_NUMBER() OVER (ORDER BY data_nascimento) AS sk_tempo,
    
    -- Metadados
    CURRENT_TIMESTAMP() AS data_criacao
    
FROM calendario_completo
ORDER BY data_nascimento
