import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import warnings
import os

# Suprimir warnings
warnings.filterwarnings("ignore")

print("🎨 GERADOR DE GRÁFICOS - HEALTH INSIGHTS BRASIL")
print("=" * 55)

# Conectar ao Snowflake
def get_snowflake_connection():
    return snowflake.connector.connect(
        user='eliasgdeveloper',
        password='%Nerd*Analist@2025#',
        account='itclrgl-zx13237',
        warehouse='COMPUTE_WH',
        database='HEALTH_INSIGHTS_DEV',
        schema='marts',
        role='ACCOUNTADMIN'
    )

# Função para executar queries
def execute_query(query, description=""):
    try:
        print(f"📊 Executando: {description}")
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        cursor.close()
        conn.close()
        print(f"   ✅ {len(df)} registros obtidos")
        return df
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        return pd.DataFrame()

# Criar pasta para gráficos
os.makedirs("graficos_projeto", exist_ok=True)

# GRÁFICO 1: Distribuição por Estados (Top 15)
print("\n🗺️ GRÁFICO 1: TOP 15 ESTADOS POR NASCIMENTOS")
query1 = """
SELECT 
    uf,
    COUNT(*) as nascimentos,
    ROUND(AVG(peso), 0) as peso_medio
FROM fct_nascimentos
GROUP BY uf
ORDER BY nascimentos DESC
LIMIT 15
"""

df_estados = execute_query(query1, "Top 15 Estados")

if not df_estados.empty:
    fig1 = px.bar(
        df_estados,
        x='UF',
        y='NASCIMENTOS',
        title='<b>Top 15 Estados - Nascimentos SINASC 2023</b>',
        color='NASCIMENTOS',
        color_continuous_scale=['#667eea', '#764ba2', '#ffd700', '#ff6b6b'],
        labels={'NASCIMENTOS': 'Número de Nascimentos', 'UF': 'Estado'}
    )
    
    fig1.update_layout(
        font=dict(size=14, family='Arial, sans-serif'),
        title_font_size=18,
        plot_bgcolor='white',
        width=1000,
        height=600,
        showlegend=False
    )
    
    # Adicionar valores nas barras
    fig1.update_traces(
        texttemplate='%{y:,.0f}',
        textposition='outside'
    )
    
    fig1.write_html("graficos_projeto/01_top_estados.html")
    fig1.write_image("graficos_projeto/01_top_estados.png", width=1000, height=600)
    print("   💾 Salvo: graficos_projeto/01_top_estados.html|png")

# GRÁFICO 2: Distribuição de Peso dos Bebês
print("\n⚖️ GRÁFICO 2: DISTRIBUIÇÃO POR CATEGORIA DE PESO")
query2 = """
SELECT 
    CASE 
        WHEN peso < 2500 THEN 'Baixo Peso (<2500g)'
        WHEN peso BETWEEN 2500 AND 4000 THEN 'Peso Normal (2500-4000g)'
        WHEN peso > 4000 THEN 'Peso Elevado (>4000g)'
        ELSE 'Não classificado'
    END as categoria_peso,
    COUNT(*) as quantidade,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 1) as percentual
FROM fct_nascimentos
GROUP BY categoria_peso
ORDER BY quantidade DESC
"""

df_peso = execute_query(query2, "Distribuição de Peso")

if not df_peso.empty:
    # Gráfico de pizza
    fig2 = px.pie(
        df_peso,
        values='QUANTIDADE',
        names='CATEGORIA_PESO',
        title='<b>Distribuição por Categoria de Peso - Indicador OMS</b>',
        color_discrete_sequence=['#38a169', '#ffd700', '#e53e3e', '#9ca3af']
    )
    
    fig2.update_traces(
        textposition='inside',
        textinfo='percent+label',
        textfont_size=12,
        marker=dict(line=dict(color='white', width=2))
    )
    
    fig2.update_layout(
        font=dict(size=14, family='Arial, sans-serif'),
        title_font_size=18,
        width=800,
        height=600
    )
    
    fig2.write_html("graficos_projeto/02_distribuicao_peso.html")
    fig2.write_image("graficos_projeto/02_distribuicao_peso.png", width=800, height=600)
    print("   💾 Salvo: graficos_projeto/02_distribuicao_peso.html|png")

# GRÁFICO 3: Evolução Temporal (Nascimentos por Mês)
print("\n📈 GRÁFICO 3: SAZONALIDADE - NASCIMENTOS POR MÊS")
query3 = """
SELECT 
    mes,
    CASE mes
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro' 
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END as mes_nome,
    COUNT(*) as nascimentos,
    ROUND(AVG(peso), 0) as peso_medio
FROM fct_nascimentos
GROUP BY mes, mes_nome
ORDER BY mes
"""

df_temporal = execute_query(query3, "Evolução Temporal")

if not df_temporal.empty:
    fig3 = px.line(
        df_temporal,
        x='MES_NOME',
        y='NASCIMENTOS',
        title='<b>Sazonalidade dos Nascimentos - Brasil 2023</b>',
        markers=True,
        line_shape='spline'
    )
    
    fig3.update_traces(
        line=dict(color='#667eea', width=4),
        marker=dict(color='#ff6b6b', size=8, line=dict(width=2, color='white'))
    )
    
    fig3.update_layout(
        font=dict(size=14, family='Arial, sans-serif'),
        title_font_size=18,
        plot_bgcolor='white',
        width=1000,
        height=600,
        xaxis_title='<b>Mês</b>',
        yaxis_title='<b>Número de Nascimentos</b>',
        xaxis=dict(tickangle=-45)
    )
    
    # Adicionar valores nos pontos
    fig3.update_traces(
        texttemplate='%{y:,.0f}',
        textposition='top center'
    )
    
    fig3.write_html("graficos_projeto/03_sazonalidade.html")
    fig3.write_image("graficos_projeto/03_sazonalidade.png", width=1000, height=600)
    print("   💾 Salvo: graficos_projeto/03_sazonalidade.html|png")

# GRÁFICO 4: Perfil Demográfico das Mães
print("\n👩‍👧‍👦 GRÁFICO 4: PERFIL ETÁRIO DAS MÃES")
query4 = """
SELECT 
    CASE 
        WHEN idade_mae < 18 THEN 'Menor de 18'
        WHEN idade_mae BETWEEN 18 AND 25 THEN '18-25 anos'
        WHEN idade_mae BETWEEN 26 AND 35 THEN '26-35 anos'
        WHEN idade_mae > 35 THEN 'Maior de 35'
        ELSE 'Não informado'
    END as faixa_etaria,
    COUNT(*) as quantidade,
    ROUND(AVG(peso), 0) as peso_medio_bebe,
    ROUND((SUM(CASE WHEN peso < 2500 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 1) as taxa_baixo_peso
FROM fct_nascimentos
WHERE idade_mae IS NOT NULL
GROUP BY faixa_etaria
ORDER BY quantidade DESC
"""

df_demografico = execute_query(query4, "Perfil Demográfico")

if not df_demografico.empty:
    # Gráfico de barras horizontal
    fig4 = px.bar(
        df_demografico,
        x='QUANTIDADE',
        y='FAIXA_ETARIA',
        orientation='h',
        title='<b>Perfil Etário das Mães - Impacto na Saúde do Bebê</b>',
        color='TAXA_BAIXO_PESO',
        color_continuous_scale=['#38a169', '#ffd700', '#e53e3e'],
        labels={
            'QUANTIDADE': 'Número de Nascimentos',
            'FAIXA_ETARIA': 'Faixa Etária',
            'TAXA_BAIXO_PESO': 'Taxa Baixo Peso (%)'
        }
    )
    
    fig4.update_layout(
        font=dict(size=14, family='Arial, sans-serif'),
        title_font_size=18,
        plot_bgcolor='white',
        width=1000,
        height=600
    )
    
    # Adicionar valores nas barras
    fig4.update_traces(
        texttemplate='%{x:,.0f}',
        textposition='outside'
    )
    
    fig4.write_html("graficos_projeto/04_perfil_demografico.html")
    fig4.write_image("graficos_projeto/04_perfil_demografico.png", width=1000, height=600)
    print("   💾 Salvo: graficos_projeto/04_perfil_demografico.html|png")

# GRÁFICO 5: Mapa de Calor - Taxa de Baixo Peso por Estado
print("\n🚨 GRÁFICO 5: MAPA DE RISCO - TAXA DE BAIXO PESO POR UF")
query5 = """
SELECT 
    uf,
    COUNT(*) as total_nascimentos,
    SUM(CASE WHEN peso < 2500 THEN 1 ELSE 0 END) as nascimentos_baixo_peso,
    ROUND((SUM(CASE WHEN peso < 2500 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 1) as taxa_baixo_peso,
    CASE 
        WHEN (SUM(CASE WHEN peso < 2500 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) > 12 THEN 'Alto Risco'
        WHEN (SUM(CASE WHEN peso < 2500 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) > 8 THEN 'Médio Risco'
        ELSE 'Baixo Risco'
    END as classificacao_risco
FROM fct_nascimentos
GROUP BY uf
ORDER BY taxa_baixo_peso DESC
"""

df_risco = execute_query(query5, "Mapa de Risco")

if not df_risco.empty:
    # Gráfico de barras com código de cores por risco
    fig5 = px.bar(
        df_risco.head(20),  # Top 20 estados
        x='UF',
        y='TAXA_BAIXO_PESO',
        title='<b>Taxa de Baixo Peso por Estado - Alertas de Saúde Pública</b>',
        color='CLASSIFICACAO_RISCO',
        color_discrete_map={
            'Alto Risco': '#e53e3e',
            'Médio Risco': '#ffd700', 
            'Baixo Risco': '#38a169'
        },
        labels={
            'TAXA_BAIXO_PESO': 'Taxa de Baixo Peso (%)',
            'UF': 'Estado',
            'CLASSIFICACAO_RISCO': 'Nível de Risco'
        }
    )
    
    # Linha de referência OMS (10%)
    fig5.add_hline(
        y=10,
        line_dash="dash",
        line_color="red",
        annotation_text="Limite OMS: 10%",
        annotation_position="top right"
    )
    
    fig5.update_layout(
        font=dict(size=14, family='Arial, sans-serif'),
        title_font_size=18,
        plot_bgcolor='white',
        width=1200,
        height=600
    )
    
    # Adicionar valores nas barras
    fig5.update_traces(
        texttemplate='%{y:.1f}%',
        textposition='outside'
    )
    
    fig5.write_html("graficos_projeto/05_mapa_risco.html")
    fig5.write_image("graficos_projeto/05_mapa_risco.png", width=1200, height=600)
    print("   💾 Salvo: graficos_projeto/05_mapa_risco.html|png")

# GRÁFICO 6: Dashboard Resumo - 4 Métricas Principais
print("\n📊 GRÁFICO 6: DASHBOARD EXECUTIVO - KPIs PRINCIPAIS")

# Query para métricas principais
query6 = """
SELECT 
    COUNT(*) as total_nascimentos,
    ROUND(AVG(peso), 0) as peso_medio,
    COUNT(DISTINCT uf) as estados_cobertos,
    ROUND(AVG(idade_mae), 1) as idade_media_mae,
    ROUND((SUM(CASE WHEN peso < 2500 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 1) as taxa_baixo_peso_nacional
FROM fct_nascimentos
"""

df_kpis = execute_query(query6, "KPIs Principais")

if not df_kpis.empty:
    # Criar subplot com 4 gráficos
    fig6 = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            'Total de Nascimentos',
            'Taxa Nacional de Baixo Peso',
            'Peso Médio dos Bebês',
            'Cobertura Nacional'
        ],
        specs=[[{"type": "indicator"}, {"type": "indicator"}],
               [{"type": "indicator"}, {"type": "indicator"}]]
    )
    
    # KPI 1: Total de Nascimentos
    fig6.add_trace(
        go.Indicator(
            mode="number",
            value=df_kpis.iloc[0]['TOTAL_NASCIMENTOS'],
            number={"font": {"size": 48, "color": "#667eea"}},
            title={"text": "registros processados", "font": {"size": 16}},
        ),
        row=1, col=1
    )
    
    # KPI 2: Taxa de Baixo Peso
    fig6.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=df_kpis.iloc[0]['TAXA_BAIXO_PESO_NACIONAL'],
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Taxa Baixo Peso (%)", "font": {"size": 16}},
            gauge={
                'axis': {'range': [0, 15]},
                'bar': {'color': "#38a169"},
                'steps': [
                    {'range': [0, 8], 'color': "#c6f6d5"},
                    {'range': [8, 10], 'color': "#ffd700"},
                    {'range': [10, 15], 'color': "#fed7d7"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 10
                }
            }
        ),
        row=1, col=2
    )
    
    # KPI 3: Peso Médio
    fig6.add_trace(
        go.Indicator(
            mode="number",
            value=df_kpis.iloc[0]['PESO_MEDIO'],
            number={"font": {"size": 48, "color": "#ffd700"}, "suffix": "g"},
            title={"text": "peso médio dos bebês", "font": {"size": 16}},
        ),
        row=2, col=1
    )
    
    # KPI 4: Cobertura
    fig6.add_trace(
        go.Indicator(
            mode="number",
            value=df_kpis.iloc[0]['ESTADOS_COBERTOS'],
            number={"font": {"size": 48, "color": "#38a169"}, "suffix": "/27"},
            title={"text": "estados cobertos", "font": {"size": 16}},
        ),
        row=2, col=2
    )
    
    fig6.update_layout(
        title={
            'text': '<b>Health Insights Brasil - Dashboard Executivo</b>',
            'font': {'size': 24},
            'x': 0.5
        },
        font=dict(family='Arial, sans-serif'),
        width=1000,
        height=800
    )
    
    fig6.write_html("graficos_projeto/06_dashboard_executivo.html")
    fig6.write_image("graficos_projeto/06_dashboard_executivo.png", width=1000, height=800)
    print("   💾 Salvo: graficos_projeto/06_dashboard_executivo.html|png")

# GRÁFICO 7: Comparação Regional (Regiões do Brasil)
print("\n🌎 GRÁFICO 7: ANÁLISE REGIONAL DO BRASIL")
query7 = """
SELECT 
    CASE 
        WHEN uf IN ('AC','AM','AP','PA','RO','RR','TO') THEN 'Norte'
        WHEN uf IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE') THEN 'Nordeste'
        WHEN uf IN ('GO','MT','MS','DF') THEN 'Centro-Oeste'
        WHEN uf IN ('ES','MG','RJ','SP') THEN 'Sudeste'
        WHEN uf IN ('PR','RS','SC') THEN 'Sul'
        ELSE 'Outros'
    END as regiao,
    COUNT(*) as nascimentos,
    ROUND(AVG(peso), 0) as peso_medio,
    ROUND((SUM(CASE WHEN peso < 2500 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 1) as taxa_baixo_peso,
    ROUND(AVG(idade_mae), 1) as idade_media_mae
FROM fct_nascimentos
WHERE uf IS NOT NULL
GROUP BY regiao
ORDER BY nascimentos DESC
"""

df_regional = execute_query(query7, "Análise Regional")

if not df_regional.empty:
    # Gráfico de barras agrupadas
    fig7 = px.bar(
        df_regional,
        x='REGIAO',
        y='NASCIMENTOS',
        title='<b>Análise Regional - Nascimentos e Indicadores de Saúde</b>',
        color='TAXA_BAIXO_PESO',
        color_continuous_scale=['#38a169', '#ffd700', '#e53e3e'],
        labels={
            'NASCIMENTOS': 'Número de Nascimentos',
            'REGIAO': 'Região',
            'TAXA_BAIXO_PESO': 'Taxa Baixo Peso (%)'
        }
    )
    
    fig7.update_layout(
        font=dict(size=14, family='Arial, sans-serif'),
        title_font_size=18,
        plot_bgcolor='white',
        width=1000,
        height=600
    )
    
    # Adicionar valores nas barras
    fig7.update_traces(
        texttemplate='%{y:,.0f}<br>%{marker.color:.1f}%',
        textposition='outside'
    )
    
    fig7.write_html("graficos_projeto/07_analise_regional.html")
    fig7.write_image("graficos_projeto/07_analise_regional.png", width=1000, height=600)
    print("   💾 Salvo: graficos_projeto/07_analise_regional.html|png")

# Gerar arquivo INDEX.html com todos os gráficos
print("\n📋 GERANDO ÍNDICE DE GRÁFICOS...")

index_html = """
<!DOCTYPE html>
<html>
<head>
    <title>Health Insights Brasil - Gráficos do Projeto</title>
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 40px; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        .header { 
            text-align: center; 
            margin-bottom: 40px; 
        }
        .chart-container { 
            margin: 30px 0; 
            padding: 20px; 
            background: rgba(255,255,255,0.1); 
            border-radius: 15px;
            backdrop-filter: blur(10px);
        }
        .chart-title { 
            font-size: 24px; 
            margin-bottom: 15px; 
            color: #ffd700;
        }
        .chart-description { 
            margin-bottom: 20px; 
            font-size: 16px;
            line-height: 1.6;
        }
        iframe { 
            width: 100%; 
            height: 650px; 
            border: none; 
            border-radius: 10px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏥 Health Insights Brasil</h1>
        <h2>Gráficos e Visualizações do Projeto</h2>
        <p><strong>Dados:</strong> SINASC 2023 | <strong>Registros:</strong> 2.537.575 nascimentos | <strong>Cobertura:</strong> 28 estados</p>
    </div>

    <div class="chart-container">
        <div class="chart-title">📊 1. Top 15 Estados por Nascimentos</div>
        <div class="chart-description">
            Distribuição geográfica dos nascimentos. SP lidera com 436k nascimentos (17,2% do país), 
            seguido por MG (233k) e BA (201k). O Sudeste concentra 47% dos nascimentos nacionais.
        </div>
        <iframe src="01_top_estados.html"></iframe>
    </div>

    <div class="chart-container">
        <div class="chart-title">⚖️ 2. Distribuição por Categoria de Peso</div>
        <div class="chart-description">
            Indicador-chave da OMS para saúde perinatal. Brasil apresenta 9,5% de baixo peso 
            (dentro do limite OMS de 10%), com 86,5% dos bebês nascendo com peso normal.
        </div>
        <iframe src="02_distribuicao_peso.html"></iframe>
    </div>

    <div class="chart-container">
        <div class="chart-title">📈 3. Sazonalidade dos Nascimentos</div>
        <div class="chart-description">
            Padrão sazonal identificado: picos em março (225k) e setembro (221k), 
            sugerindo concepções em junho/dezembro. Variação de até 12% entre meses.
        </div>
        <iframe src="03_sazonalidade.html"></iframe>
    </div>

    <div class="chart-container">
        <div class="chart-title">👩‍👧‍👦 4. Perfil Etário das Mães</div>
        <div class="chart-description">
            Mães de 26-35 anos representam 48,7% dos nascimentos com menor taxa de baixo peso (8,1%). 
            Adolescentes (<18 anos) apresentam maior risco com 15,3% de baixo peso.
        </div>
        <iframe src="04_perfil_demografico.html"></iframe>
    </div>

    <div class="chart-container">
        <div class="chart-title">🚨 5. Mapa de Risco - Taxa de Baixo Peso por Estado</div>
        <div class="chart-description">
            Sistema de alertas para gestores de saúde pública. Estados com taxa >10% (limite OMS) 
            requerem atenção especial. Classificação automática em Alto/Médio/Baixo risco.
        </div>
        <iframe src="05_mapa_risco.html"></iframe>
    </div>

    <div class="chart-container">
        <div class="chart-title">📊 6. Dashboard Executivo - KPIs Principais</div>
        <div class="chart-description">
            Métricas executivas: 2,5M+ nascimentos processados, taxa nacional de baixo peso de 9,5% 
            (dentro do padrão OMS), peso médio de 3.151g e cobertura de 28 estados.
        </div>
        <iframe src="06_dashboard_executivo.html"></iframe>
    </div>

    <div class="chart-container">
        <div class="chart-title">🌎 7. Análise Regional do Brasil</div>
        <div class="chart-description">
            Comparação entre as 5 regiões brasileiras. Sudeste lidera em volume (47% dos nascimentos), 
            Norte apresenta maior taxa de baixo peso, Sul tem melhores indicadores de saúde.
        </div>
        <iframe src="07_analise_regional.html"></iframe>
    </div>

    <div style="text-align: center; margin-top: 50px; padding: 30px; background: rgba(255,255,255,0.1); border-radius: 15px;">
        <h3>🏆 Health Insights Brasil</h3>
        <p><strong>Projeto Completo de Engenharia de Dados</strong></p>
        <p>Snowflake + dbt Cloud + Streamlit + Python</p>
        <p><em>Transformando dados em insights para a saúde pública brasileira</em> 🇧🇷</p>
    </div>
</body>
</html>
"""

with open("graficos_projeto/index.html", "w", encoding="utf-8") as f:
    f.write(index_html)

print("   💾 Salvo: graficos_projeto/index.html")

print(f"\n🎉 GRÁFICOS GERADOS COM SUCESSO!")
print(f"📁 Pasta: graficos_projeto/")
print(f"📊 Total: 7 gráficos + 1 índice")
print(f"🌐 Para visualizar: Abra graficos_projeto/index.html no browser")

# Listar arquivos gerados
import glob
arquivos = glob.glob("graficos_projeto/*")
print(f"\n📋 ARQUIVOS GERADOS:")
for arquivo in sorted(arquivos):
    print(f"   📄 {arquivo}")

print(f"\n💡 COMO USAR NOS DOCUMENTOS:")
print(f"1. Abra os arquivos .png no editor de imagens")
print(f"2. Cole nos documentos Word/PowerPoint") 
print(f"3. Use os .html para apresentações interativas")
print(f"4. Inclua o index.html como anexo completo")

print(f"\n🎯 PRÓXIMO PASSO:")
print(f"Execute: start graficos_projeto/index.html (Windows)")
print(f"Ou abra manualmente o arquivo index.html no navegador")
