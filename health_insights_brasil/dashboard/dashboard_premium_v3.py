import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import warnings
from datetime import datetime

# Suprimir warnings
warnings.filterwarnings("ignore")

# Configuração da página
st.set_page_config(
    page_title="Health Insights Brasil - SINASC 2023",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado com cores mais vibrantes
st.markdown("""
<style>
    /* Importar Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    .stApp {
        font-family: 'Inter', sans-serif;
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 50%, #667eea 100%);
        color: white;
    }
    
    /* Sidebar com cores mais escuras */
    .css-1d391kg {
        background: linear-gradient(180deg, #1a202c 0%, #2d3748 100%) !important;
    }
    
    /* Cards de métricas com cores mais vibrantes */
    .metric-card {
        background: linear-gradient(145deg, #ffffff 0%, #f7fafc 100%);
        padding: 2rem;
        border-radius: 20px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        margin: 1rem 0;
        text-align: center;
        border: 2px solid rgba(255,255,255,0.1);
        transition: all 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-8px);
        box-shadow: 0 15px 40px rgba(0,0,0,0.3);
    }
    
    .big-number {
        font-size: 3rem;
        font-weight: 700;
        color: #1a202c;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    
    .metric-label {
        color: #4a5568;
        font-size: 1.2rem;
        font-weight: 600;
        margin-top: 0.5rem;
    }
    
    /* Título principal mais chamativo */
    .main-header {
        text-align: center;
        color: #ffffff;
        font-size: 4rem;
        font-weight: 800;
        margin-bottom: 1rem;
        text-shadow: 3px 3px 6px rgba(0,0,0,0.3);
        background: linear-gradient(45deg, #ffd700, #ffed4a, #68d391);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    .sub-header {
        text-align: center;
        color: #e2e8f0;
        font-size: 1.4rem;
        margin-bottom: 2rem;
        font-weight: 500;
    }
    
    /* Caixas de status melhoradas */
    .error-box {
        background: linear-gradient(135deg, #fed7d7, #feb2b2);
        color: #742a2a;
        padding: 1.5rem;
        border-radius: 15px;
        border-left: 5px solid #e53e3e;
        margin: 1rem 0;
        font-weight: 500;
    }
    
    .success-box {
        background: linear-gradient(135deg, #c6f6d5, #9ae6b4);
        color: #22543d;
        padding: 1.5rem;
        border-radius: 15px;
        border-left: 5px solid #38a169;
        margin: 1rem 0;
        font-weight: 500;
    }
    
    /* Melhorar visibilidade de texto em gráficos */
    .js-plotly-plot .plotly {
        background: transparent !important;
    }
    
    /* Sidebar styling */
    .stSelectbox label {
        color: #e2e8f0 !important;
        font-weight: 600 !important;
    }
    
    .stButton button {
        background: linear-gradient(45deg, #667eea, #764ba2);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.5rem 1rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
    }
    
    /* Headers de seções */
    h2, h3 {
        color: #ffffff !important;
        font-weight: 700 !important;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3) !important;
    }
</style>
""", unsafe_allow_html=True)

# Título
st.markdown('<h1 class="main-header">🏥 Health Insights Brasil</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">📊 Dashboard Premium de Análise de Nascimentos - SINASC 2023</p>', unsafe_allow_html=True)

# Função para conexão com Snowflake
@st.cache_resource(show_spinner=True)
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"],
            role=st.secrets["snowflake"]["role"]
        )
        return conn
    except Exception as e:
        st.error(f"❌ Erro na conexão com Snowflake: {str(e)}")
        return None

# Função para executar queries
@st.cache_data(ttl=300, show_spinner=True)
def execute_query(query):
    try:
        conn = get_snowflake_connection()
        if conn is None:
            return None
        
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Buscar dados
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        cursor.close()
        return df
        
    except Exception as e:
        st.error(f"❌ Erro na execução da query: {str(e)}")
        return None

# Verificar conexão
with st.spinner("🔄 Conectando ao Snowflake..."):
    connection_test = get_snowflake_connection()

if connection_test is None:
    st.markdown("""
    <div class="error-box">
        <h3>⚠️ Problema de Conexão</h3>
        <p>Não foi possível conectar ao Snowflake. Verifique:</p>
        <ul>
            <li>Credenciais no arquivo secrets.toml</li>
            <li>Conexão com a internet</li>
            <li>Permissões de acesso ao banco</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    st.stop()
else:
    st.markdown("""
    <div class="success-box">
        <p>✅ <strong>Conexão estabelecida com sucesso!</strong> Sistema pronto para análise.</p>
    </div>
    """, unsafe_allow_html=True)

# Sidebar com controles
st.sidebar.markdown("## 🎛️ **Controles do Dashboard**")

# Filtro de UF
st.sidebar.markdown("### 🌎 **Filtros Geográficos**")
uf_query = "SELECT DISTINCT uf FROM fct_nascimentos WHERE uf IS NOT NULL ORDER BY uf"
uf_data = execute_query(uf_query)

if uf_data is not None and not uf_data.empty:
    uf_options = ["Todos"] + uf_data['UF'].tolist()
    selected_uf = st.sidebar.selectbox("**Estado (UF)**", uf_options)
else:
    selected_uf = "Todos"

# Filtro de período
st.sidebar.markdown("### 📅 **Filtros Temporais**")
periodo_options = ["Todos", "1º Trimestre", "2º Trimestre", "3º Trimestre", "4º Trimestre"]
selected_periodo = st.sidebar.selectbox("**Período**", periodo_options)

# Construir WHERE clause
where_conditions = []
if selected_uf != "Todos":
    where_conditions.append(f"uf = '{selected_uf}'")

if selected_periodo != "Todos":
    if selected_periodo == "1º Trimestre":
        where_conditions.append("mes BETWEEN 1 AND 3")
    elif selected_periodo == "2º Trimestre":
        where_conditions.append("mes BETWEEN 4 AND 6")
    elif selected_periodo == "3º Trimestre":
        where_conditions.append("mes BETWEEN 7 AND 9")
    elif selected_periodo == "4º Trimestre":
        where_conditions.append("mes BETWEEN 10 AND 12")

where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

# Botão de atualizar
if st.sidebar.button("🔄 **Atualizar Dados**"):
    st.cache_data.clear()
    st.rerun()

# Query principal para métricas
metrics_query = f"""
SELECT 
    COUNT(*) as total_nascimentos,
    ROUND(AVG(peso), 0) as peso_medio,
    COUNT(DISTINCT uf) as total_estados,
    ROUND(AVG(idade_mae), 1) as idade_media_mae,
    SUM(CASE WHEN peso < 2500 THEN 1 ELSE 0 END) as baixo_peso
FROM fct_nascimentos 
{where_clause}
"""

# Executar query principal
with st.spinner("📊 Carregando métricas principais..."):
    metrics_df = execute_query(metrics_query)

if metrics_df is not None and not metrics_df.empty:
    # Exibir métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total = int(metrics_df.iloc[0]['TOTAL_NASCIMENTOS'])
        st.markdown(f"""
        <div class="metric-card">
            <div class="big-number">{total:,}</div>
            <div class="metric-label">👶 Nascimentos</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        peso = int(metrics_df.iloc[0]['PESO_MEDIO'])
        st.markdown(f"""
        <div class="metric-card">
            <div class="big-number">{peso}g</div>
            <div class="metric-label">⚖️ Peso Médio</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        estados = int(metrics_df.iloc[0]['TOTAL_ESTADOS'])
        st.markdown(f"""
        <div class="metric-card">
            <div class="big-number">{estados}</div>
            <div class="metric-label">🌎 Estados</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        idade = float(metrics_df.iloc[0]['IDADE_MEDIA_MAE'])
        st.markdown(f"""
        <div class="metric-card">
            <div class="big-number">{idade}</div>
            <div class="metric-label">👩 Idade Média Mães</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Seção de análises
    st.markdown("---")
    
    # Análise temporal com cores mais vibrantes
    st.subheader("📈 Análise Temporal")
    temporal_query = f"""
    SELECT 
        mes,
        COUNT(*) as nascimentos
    FROM fct_nascimentos 
    {where_clause}
    GROUP BY mes
    ORDER BY mes
    """
    
    with st.spinner("Carregando dados temporais..."):
        temporal_df = execute_query(temporal_query)
    
    if temporal_df is not None and not temporal_df.empty:
        fig_temporal = px.line(
            temporal_df, 
            x='MES', 
            y='NASCIMENTOS',
            title="<b>Nascimentos por Mês - 2023</b>",
            markers=True
        )
        fig_temporal.update_traces(
            line=dict(color='#ffd700', width=4),
            marker=dict(color='#ff6b6b', size=10, line=dict(width=2, color='white'))
        )
        fig_temporal.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white', size=14, family='Inter'),
            title_font=dict(size=20, color='white'),
            xaxis_title="<b>Mês</b>",
            yaxis_title="<b>Número de Nascimentos</b>",
            xaxis=dict(gridcolor='rgba(255,255,255,0.2)'),
            yaxis=dict(gridcolor='rgba(255,255,255,0.2)')
        )
        st.plotly_chart(fig_temporal, use_container_width=True)
    
    # Análise geográfica
    st.subheader("🗺️ Análise por Estados")
    
    col1, col2 = st.columns(2)
    
    with col1:
        geo_query = f"""
        SELECT 
            uf,
            COUNT(*) as nascimentos
        FROM fct_nascimentos 
        {where_clause}
        GROUP BY uf
        ORDER BY nascimentos DESC
        LIMIT 10
        """
        
        with st.spinner("Carregando dados geográficos..."):
            geo_df = execute_query(geo_query)
        
        if geo_df is not None and not geo_df.empty:
            fig_geo = px.bar(
                geo_df,
                x='UF',
                y='NASCIMENTOS',
                title="<b>Top 10 Estados - Nascimentos</b>",
                color='NASCIMENTOS',
                color_continuous_scale=['#667eea', '#764ba2', '#ffd700', '#ff6b6b']
            )
            fig_geo.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white', size=14, family='Inter'),
                title_font=dict(size=18, color='white'),
                xaxis=dict(gridcolor='rgba(255,255,255,0.2)'),
                yaxis=dict(gridcolor='rgba(255,255,255,0.2)'),
                showlegend=False
            )
            st.plotly_chart(fig_geo, use_container_width=True)
    
    with col2:
        # Análise de peso com cores mais chamativas
        peso_query = f"""
        SELECT 
            CASE 
                WHEN peso < 2500 THEN 'Baixo Peso'
                WHEN peso BETWEEN 2500 AND 4000 THEN 'Peso Normal'
                ELSE 'Peso Elevado'
            END as categoria_peso,
            COUNT(*) as quantidade
        FROM fct_nascimentos 
        {where_clause}
        GROUP BY categoria_peso
        ORDER BY quantidade DESC
        """
        
        with st.spinner("Carregando análise de peso..."):
            peso_df = execute_query(peso_query)
        
        if peso_df is not None and not peso_df.empty:
            fig_peso = px.pie(
                peso_df,
                values='QUANTIDADE',
                names='CATEGORIA_PESO',
                title="<b>Distribuição por Categoria de Peso</b>",
                color_discrete_sequence=['#38a169', '#ffd700', '#e53e3e']
            )
            fig_peso.update_traces(
                textposition='inside', 
                textinfo='percent+label',
                textfont_size=14,
                marker=dict(line=dict(color='white', width=3))
            )
            fig_peso.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white', size=14, family='Inter'),
                title_font=dict(size=18, color='white')
            )
            st.plotly_chart(fig_peso, use_container_width=True)
    
    # Análise demográfica das mães
    st.subheader("👩‍👧‍👦 Perfil das Mães")
    
    idade_query = f"""
    SELECT 
        CASE 
            WHEN idade_mae < 18 THEN 'Menor de 18'
            WHEN idade_mae BETWEEN 18 AND 25 THEN '18-25 anos'
            WHEN idade_mae BETWEEN 26 AND 35 THEN '26-35 anos'
            WHEN idade_mae > 35 THEN 'Maior de 35'
            ELSE 'Não informado'
        END as faixa_etaria,
        COUNT(*) as quantidade
    FROM fct_nascimentos 
    {where_clause}
    GROUP BY faixa_etaria
    ORDER BY quantidade DESC
    """
    
    with st.spinner("Carregando dados demográficos..."):
        idade_df = execute_query(idade_query)
    
    if idade_df is not None and not idade_df.empty:
        fig_idade = px.bar(
            idade_df,
            x='QUANTIDADE',
            y='FAIXA_ETARIA',
            orientation='h',
            title="<b>Distribuição por Faixa Etária das Mães</b>",
            color='QUANTIDADE',
            color_continuous_scale=['#667eea', '#764ba2', '#ffd700', '#ff6b6b', '#38a169']
        )
        fig_idade.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white', size=14, family='Inter'),
            title_font=dict(size=18, color='white'),
            xaxis=dict(gridcolor='rgba(255,255,255,0.2)'),
            yaxis=dict(gridcolor='rgba(255,255,255,0.2)'),
            showlegend=False,
            xaxis_title="<b>Número de Nascimentos</b>",
            yaxis_title="<b>Faixa Etária</b>"
        )
        st.plotly_chart(fig_idade, use_container_width=True)

else:
    st.error("❌ Não foi possível carregar as métricas principais.")

# Footer melhorado
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: #e2e8f0; padding: 30px; background: linear-gradient(45deg, rgba(26,32,44,0.8), rgba(45,55,72,0.8)); border-radius: 15px; margin-top: 2rem;'>
    <h3 style='color: #ffd700; margin-bottom: 1rem;'>🏥 Health Insights Brasil</h3>
    <p><strong>Plataforma Completa de Análise de Saúde Pública</strong></p>
    <p>🔧 Desenvolvido por ELIAS com dbt Cloud + Snowflake + Streamlit | 📊 Dashboard Premium v3.0</p>
    <p>📈 Dados: SINASC 2023 - 2,5M+ registros | 🕐 Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
</div>
""", unsafe_allow_html=True)
