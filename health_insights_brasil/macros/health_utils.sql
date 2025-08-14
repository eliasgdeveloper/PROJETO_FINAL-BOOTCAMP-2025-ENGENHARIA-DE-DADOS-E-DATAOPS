-- Macro para categorização de peso segundo OMS
-- Autor: ELIAS
-- Projeto: Health Insights Brasil

{% macro categorizar_peso(peso_column) %}
    case 
        when {{ peso_column }} < 2500 then 'Baixo Peso'
        when {{ peso_column }} between 2500 and 4000 then 'Peso Normal'
        when {{ peso_column }} > 4000 then 'Peso Elevado'
        else 'Não Informado'
    end
{% endmacro %}

-- Macro para classificação de faixa etária
{% macro classificar_idade_mae(idade_column) %}
    case 
        when {{ idade_column }} < 18 then 'Adolescente'
        when {{ idade_column }} between 18 and 25 then 'Jovem Adulta'
        when {{ idade_column }} between 26 and 35 then 'Adulta'
        when {{ idade_column }} > 35 then 'Adulta Tardia'
        else 'Não Informado'
    end
{% endmacro %}

-- Macro para mapeamento de regiões brasileiras
{% macro mapear_regiao(uf_column) %}
    case 
        -- Norte
        when {{ uf_column }} in ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') then 'Norte'
        -- Nordeste  
        when {{ uf_column }} in ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') then 'Nordeste'
        -- Centro-Oeste
        when {{ uf_column }} in ('GO', 'MT', 'MS', 'DF') then 'Centro-Oeste'
        -- Sudeste
        when {{ uf_column }} in ('ES', 'MG', 'RJ', 'SP') then 'Sudeste'
        -- Sul
        when {{ uf_column }} in ('PR', 'RS', 'SC') then 'Sul'
        else 'Não Identificado'
    end
{% endmacro %}

-- Macro para indicador de risco
{% macro calcular_indicador_risco(peso_column, idade_mae_column) %}
    case 
        when {{ peso_column }} < 2500 or {{ idade_mae_column }} < 18 or {{ idade_mae_column }} > 40 then 'Alto Risco'
        when {{ peso_column }} between 2500 and 3000 or {{ idade_mae_column }} between 35 and 40 then 'Médio Risco'
        else 'Baixo Risco'
    end
{% endmacro %}
