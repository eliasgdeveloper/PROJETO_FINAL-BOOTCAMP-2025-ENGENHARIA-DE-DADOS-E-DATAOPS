# Health Insights Brasil - Documentação dbt

**Autor:** ELIAS  
**Projeto:** Análise de Dados de Nascimentos SINASC 2023  
**Versão:** 1.0.0

## Visão Geral

Este projeto dbt processa e transforma dados do Sistema de Informações sobre Nascidos Vivos (SINASC) de 2023, criando um pipeline de dados robusto para análises de saúde pública no Brasil.

## Arquitetura dos Dados

### 📊 **Sources (Fontes)**
- **raw.SINASC_RAW**: Dados brutos importados do CSV original (2.5M+ registros)

### 🔄 **Staging Models**
- **stg_sinasc**: Limpeza e padronização dos dados brutos
  - Conversão de tipos de dados
  - Padronização de valores categóricos
  - Tratamento de valores nulos

### 📈 **Marts Models**
- **fct_nascimentos**: Tabela de fatos com métricas calculadas
  - Categorizações de saúde (peso, idade materna)
  - Mapeamento regional
  - Indicadores de risco
  - Dimensões temporais

## Macros Disponíveis

### `categorizar_peso(peso_column)`
Categoriza o peso do bebê segundo padrões da OMS:
- **Baixo Peso**: < 2500g
- **Peso Normal**: 2500-4000g  
- **Peso Elevado**: > 4000g

### `classificar_idade_mae(idade_column)`
Classifica a idade materna em faixas etárias:
- **Adolescente**: < 18 anos
- **Jovem Adulta**: 18-25 anos
- **Adulta**: 26-35 anos
- **Adulta Tardia**: > 35 anos

### `mapear_regiao(uf_column)`
Mapeia estados para regiões brasileiras:
- **Norte**: AC, AP, AM, PA, RO, RR, TO
- **Nordeste**: AL, BA, CE, MA, PB, PE, PI, RN, SE
- **Centro-Oeste**: GO, MT, MS, DF
- **Sudeste**: ES, MG, RJ, SP
- **Sul**: PR, RS, SC

### `calcular_indicador_risco(peso_column, idade_mae_column)`
Calcula indicador de risco baseado em peso e idade materna:
- **Alto Risco**: Peso < 2500g OU idade < 18 OU idade > 40
- **Médio Risco**: Peso 2500-3000g OU idade 35-40
- **Baixo Risco**: Demais casos

## Testes de Qualidade

### Testes Automáticos
- **Unicidade**: IDs únicos em todas as tabelas
- **Não Nulos**: Campos obrigatórios
- **Valores Aceitos**: Validação de domínios
- **Ranges**: Validação de intervalos (peso, idade)

### Testes Customizados
- **assert_peso_consistente**: Verifica consistência dos dados de peso
- **assert_distribuicao_regional**: Valida representatividade regional

## Configurações

### Materialização
- **Staging**: Views (para economia de storage)
- **Marts**: Tabelas (para performance de consulta)

### Variáveis
```yaml
vars:
  start_date: '2023-01-01'
  end_date: '2023-12-31'
  min_peso: 500
  max_peso: 6000
  min_idade_mae: 10
  max_idade_mae: 60
```

## Comandos Úteis

```bash
# Executar todos os modelos
dbt run

# Executar apenas staging
dbt run --models staging

# Executar apenas marts  
dbt run --models marts

# Executar testes
dbt test

# Gerar documentação
dbt docs generate
dbt docs serve

# Executar com perfil específico
dbt run --profile health_insights_brasil --target prod
```

## Pipeline de Dados

```
SINASC_RAW → stg_sinasc → fct_nascimentos → Dashboard Streamlit
```

### Fluxo de Transformação
1. **Extração**: Dados brutos do SINASC 2023
2. **Limpeza**: Padronização e validação (staging)
3. **Transformação**: Cálculos e categorizações (marts)
4. **Consumo**: Dashboard interativo e visualizações

## Métricas Principais

- **Volume**: 2.5M+ nascimentos processados
- **Cobertura**: Todos os estados brasileiros
- **Qualidade**: 99%+ de dados válidos após limpeza
- **Performance**: Pipeline executa em < 5 minutos

## Contato

Para dúvidas ou sugestões sobre este projeto dbt, entre em contato com **ELIAS**.
