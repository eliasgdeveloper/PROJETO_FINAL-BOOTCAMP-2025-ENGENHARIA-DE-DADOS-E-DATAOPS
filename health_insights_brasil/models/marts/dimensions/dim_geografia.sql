-- Dimensão geográfica com hierarquias administrativas e análises regionais
{{ config(
    materialized='table',
    tags=['marts', 'dimensions']
) }}

WITH geografia_base AS (
    SELECT DISTINCT 
        codmunres,
        munres AS municipio_nascimento,
        uf AS estado
    FROM {{ ref('stg_sinasc') }}
    WHERE codmunres IS NOT NULL
),

regioes_brasil AS (
    SELECT 
        *,
        -- Classificação por região
        CASE 
            WHEN estado IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
            WHEN estado IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
            WHEN estado IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
            WHEN estado IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
            WHEN estado IN ('PR', 'RS', 'SC') THEN 'Sul'
            ELSE 'Não Classificado'
        END AS regiao,
        
        -- Nome completo dos estados
        CASE estado
            WHEN 'AC' THEN 'Acre' WHEN 'AL' THEN 'Alagoas' WHEN 'AP' THEN 'Amapá'
            WHEN 'AM' THEN 'Amazonas' WHEN 'BA' THEN 'Bahia' WHEN 'CE' THEN 'Ceará'
            WHEN 'DF' THEN 'Distrito Federal' WHEN 'ES' THEN 'Espírito Santo'
            WHEN 'GO' THEN 'Goiás' WHEN 'MA' THEN 'Maranhão' WHEN 'MT' THEN 'Mato Grosso'
            WHEN 'MS' THEN 'Mato Grosso do Sul' WHEN 'MG' THEN 'Minas Gerais'
            WHEN 'PA' THEN 'Pará' WHEN 'PB' THEN 'Paraíba' WHEN 'PR' THEN 'Paraná'
            WHEN 'PE' THEN 'Pernambuco' WHEN 'PI' THEN 'Piauí' WHEN 'RJ' THEN 'Rio de Janeiro'
            WHEN 'RN' THEN 'Rio Grande do Norte' WHEN 'RS' THEN 'Rio Grande do Sul'
            WHEN 'RO' THEN 'Rondônia' WHEN 'RR' THEN 'Roraima' WHEN 'SC' THEN 'Santa Catarina'
            WHEN 'SP' THEN 'São Paulo' WHEN 'SE' THEN 'Sergipe' WHEN 'TO' THEN 'Tocantins'
            ELSE estado
        END AS nome_estado,
        
        -- Classificações por desenvolvimento
        CASE 
            WHEN estado IN ('SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'ES', 'GO', 'DF') THEN 'Desenvolvido'
            WHEN estado IN ('BA', 'PE', 'CE', 'PA', 'MT', 'MS') THEN 'Em Desenvolvimento'
            ELSE 'Emergente'
        END AS nivel_desenvolvimento,
        
        -- Classificação por tamanho populacional (aproximado)
        CASE 
            WHEN estado IN ('SP', 'MG', 'RJ', 'BA', 'PR', 'RS') THEN 'Grande'
            WHEN estado IN ('PE', 'CE', 'PA', 'SC', 'GO', 'PB', 'ES', 'RN', 'MT') THEN 'Médio'
            ELSE 'Pequeno'
        END AS porte_populacional,
        
        -- Faixa litorânea
        CASE 
            WHEN estado IN ('RS', 'SC', 'PR', 'SP', 'RJ', 'ES', 'BA', 'SE', 'AL', 
                          'PE', 'PB', 'RN', 'CE', 'PI', 'MA', 'PA', 'AP') THEN TRUE
            ELSE FALSE
        END AS estado_litoraneo
        
    FROM geografia_base
),

estatisticas_municipio AS (
    SELECT 
        rg.*,
        
        -- Estatísticas do município no dataset
        COUNT(*) OVER (PARTITION BY codmunres) AS total_nascimentos_municipio,
        RANK() OVER (PARTITION BY estado ORDER BY COUNT(*) OVER (PARTITION BY codmunres) DESC) AS rank_municipio_estado,
        
        -- Identificação de capitais (principais)
        CASE 
            WHEN (estado = 'SP' AND municipio_nascimento LIKE '%SAO PAULO%') OR
                 (estado = 'RJ' AND municipio_nascimento LIKE '%RIO DE JANEIRO%') OR
                 (estado = 'MG' AND municipio_nascimento LIKE '%BELO HORIZONTE%') OR
                 (estado = 'BA' AND municipio_nascimento LIKE '%SALVADOR%') OR
                 (estado = 'PR' AND municipio_nascimento LIKE '%CURITIBA%') OR
                 (estado = 'RS' AND municipio_nascimento LIKE '%PORTO ALEGRE%') OR
                 (estado = 'PE' AND municipio_nascimento LIKE '%RECIFE%') OR
                 (estado = 'CE' AND municipio_nascimento LIKE '%FORTALEZA%') OR
                 (estado = 'DF' AND municipio_nascimento LIKE '%BRASILIA%') OR
                 (estado = 'GO' AND municipio_nascimento LIKE '%GOIANIA%') OR
                 (estado = 'SC' AND municipio_nascimento LIKE '%FLORIANOPOLIS%') OR
                 (estado = 'ES' AND municipio_nascimento LIKE '%VITORIA%') OR
                 (estado = 'PA' AND municipio_nascimento LIKE '%BELEM%') OR
                 (estado = 'MA' AND municipio_nascimento LIKE '%SAO LUIS%') OR
                 (estado = 'PB' AND municipio_nascimento LIKE '%JOAO PESSOA%') OR
                 (estado = 'AL' AND municipio_nascimento LIKE '%MACEIO%') OR
                 (estado = 'SE' AND municipio_nascimento LIKE '%ARACAJU%') OR
                 (estado = 'RN' AND municipio_nascimento LIKE '%NATAL%') OR
                 (estado = 'PI' AND municipio_nascimento LIKE '%TERESINA%') OR
                 (estado = 'MT' AND municipio_nascimento LIKE '%CUIABA%') OR
                 (estado = 'MS' AND municipio_nascimento LIKE '%CAMPO GRANDE%') OR
                 (estado = 'RO' AND municipio_nascimento LIKE '%PORTO VELHO%') OR
                 (estado = 'AC' AND municipio_nascimento LIKE '%RIO BRANCO%') OR
                 (estado = 'AM' AND municipio_nascimento LIKE '%MANAUS%') OR
                 (estado = 'RR' AND municipio_nascimento LIKE '%BOA VISTA%') OR
                 (estado = 'AP' AND municipio_nascimento LIKE '%MACAPA%') OR
                 (estado = 'TO' AND municipio_nascimento LIKE '%PALMAS%')
            THEN TRUE 
            ELSE FALSE 
        END AS eh_capital
        
    FROM regioes_brasil rg
)

SELECT 
    -- Chaves
    ROW_NUMBER() OVER (ORDER BY codmunres) AS sk_geografia,
    codmunres,
    
    -- Hierarquia geográfica
    municipio_nascimento,
    estado,
    nome_estado,
    regiao,
    
    -- Classificações
    nivel_desenvolvimento,
    porte_populacional,
    estado_litoraneo,
    eh_capital,
    
    -- Estatísticas
    total_nascimentos_municipio,
    rank_municipio_estado,
    
    -- Agrupamentos para análise
    CASE WHEN eh_capital THEN 'Capital' 
         WHEN rank_municipio_estado <= 5 THEN 'Cidade Principal'
         ELSE 'Interior' 
    END AS classificacao_urbana,
    
    -- Metadados
    CURRENT_TIMESTAMP() AS data_criacao,
    'ELIAS' AS criado_por
    
FROM estatisticas_municipio
ORDER BY regiao, estado, total_nascimentos_municipio DESC
