# Arquitetura e Modelos dbt - Health Insights Brasil

## 📊 Estrutura Dimensional

### **Camada Staging**
- **`stg_sinasc.sql`**: Limpeza e padronização dos dados brutos do SINASC

### **Camada Marts - Core**
- **`fct_nascimentos.sql`**: Tabela fato principal com métricas de nascimentos
- **`dim_tempo.sql`**: Dimensão temporal com hierarquias e sazonalidade  
- **`dim_geografia.sql`**: Dimensão geográfica com regiões e classificações
- **`dim_demografia_mae.sql`**: Perfil demográfico materno detalhado
- **`dim_medico.sql`**: Características médicas do parto e condições clínicas

### **Camada Analytics**
- **`agg_nascimentos_completa.sql`**: Agregações para análises de performance
- **`analise_epidemiologica.sql`**: Análises de saúde pública e desigualdades
- **`indicadores_qualidade.sql`**: Métricas de qualidade assistencial

### **Camada Utilities**
- **`metricas_pipeline.sql`**: Monitoramento e performance do pipeline
- **`testes_qualidade.sql`**: Validações e testes automatizados

## 🏗️ Modelo Dimensional

```
fct_nascimentos (Fato Central)
├── sk_geografia → dim_geografia
├── sk_demografia → dim_demografia_mae  
├── sk_medico → dim_medico
└── data_nascimento → dim_tempo
```

## 📈 Principais Análises Suportadas

### **1. Epidemiológica**
- Desigualdades sociais em saúde
- Variações geográficas de indicadores
- Tendências temporais e sazonalidade
- Fatores de risco materno-infantil

### **2. Qualidade Assistencial**
- Indicadores por Classificação de Robson
- Taxas de cesárea e adequação (OMS: 10-15%)
- Cobertura de pré-natal (meta: >80%)
- Outcomes neonatais (APGAR, peso, prematuridade)

### **3. Performance Operacional**
- Métricas de pipeline e qualidade de dados
- Monitoramento de integridade referencial
- Detecção de outliers e inconsistências
- Cobertura temporal e geográfica

## 🔍 Indicadores-Chave (KPIs)

| **Categoria** | **Indicador** | **Meta/Referência** |
|---------------|---------------|-------------------|
| **Parto** | Taxa de Cesárea | 10-15% (OMS) |
| **Pré-natal** | Consultas Adequadas | ≥80% |
| **Neonatal** | APGAR 5min Normal | ≥95% |
| **Peso** | Baixo Peso ao Nascer | <10% |
| **Gestação** | Prematuridade | <10% |

## 🏷️ Tags de Organização

- **`staging`**: Modelos de limpeza inicial
- **`marts`**: Modelos dimensionais core  
- **`dimensions`**: Tabelas de dimensão
- **`agregacoes`**: Sumarizações analíticas
- **`analises`**: Análises epidemiológicas
- **`qualidade`**: Indicadores assistenciais
- **`utils`**: Utilitários e monitoramento

## 🔄 Fluxo de Execução

1. **Staging**: `stg_sinasc` (limpeza)
2. **Dimensões**: `dim_*` (classificações e hierarquias)
3. **Fato**: `fct_nascimentos` (métricas centrais)
4. **Analytics**: Agregações e análises especializadas
5. **Utils**: Monitoramento e validação contínua

## 📋 Metadados de Qualidade

Todos os modelos incluem:
- **Autor**: ELIAS
- **Data de criação**: Timestamp automático
- **Tags**: Organização por domínio
- **Testes**: Validações automatizadas
- **Documentação**: Descrições inline detalhadas

---
*Estrutura criada para demonstrar capacidades avançadas de modelagem dimensional, análises epidemiológicas e engenharia de dados para o domínio de saúde pública.*
