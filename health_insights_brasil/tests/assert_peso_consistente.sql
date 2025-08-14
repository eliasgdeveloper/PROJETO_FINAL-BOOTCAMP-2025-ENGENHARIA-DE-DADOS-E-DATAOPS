-- Teste para verificar consistência de dados de peso
-- Autor: ELIAS
-- Projeto: Health Insights Brasil

-- tests/assert_peso_consistente.sql
select 
    count(*) as registros_inconsistentes
from {{ ref('stg_sinasc') }}
where peso is null 
   or peso < 500 
   or peso > 6000

-- Este teste deve retornar 0 registros para passar
