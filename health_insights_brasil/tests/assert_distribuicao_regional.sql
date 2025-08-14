-- Teste para verificar distribuição de nascimentos por região
-- Autor: ELIAS
-- Projeto: Health Insights Brasil

select 
    regiao,
    count(*) as total_nascimentos,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentual
from {{ ref('fct_nascimentos') }}
group by regiao
having count(*) < 1000  -- Falha se alguma região tem menos de 1000 nascimentos

-- Este teste verifica se todas as regiões têm representação adequada
