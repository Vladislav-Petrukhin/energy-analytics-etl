"""
Streamlit Dashboard для визуализации энергетической аналитики
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import os

# Настройка страницы
st.set_page_config(
    page_title="Энергетическая аналитика",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Параметры подключения к базе данных
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'energy_analytics'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

def get_db_connection():
    """Создать новое подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных: {e}")
        return None

def load_data(query, params=None):
    """Загрузить данные из PostgreSQL"""
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        if params:
            df = pd.read_sql_query(query, conn, params=params)
        else:
            df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Ошибка при выполнении запроса: {e}")
        return None
    finally:
        if conn:
            conn.close()

# Заголовок
st.title("⚡ Энергетическая аналитика")
st.markdown("---")

# Боковая панель с фильтрами
st.sidebar.header("Фильтры")

# Кнопка для показа всех данных
show_all_data = st.sidebar.checkbox("Показать все данные (без фильтра)", value=True)

# Выбор временного периода (опционально, если не выбрано "показать все")
if not show_all_data:
    date_range = st.sidebar.date_input(
        "Выберите период",
        value=(datetime(2020, 1, 1), datetime(2030, 12, 31)),
        min_value=datetime(2020, 1, 1),
        max_value=datetime(2030, 12, 31)
    )
else:
    date_range = None  # Показываем все данные

# Основной контент
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Обзор",
    "⏰ Почасовой анализ",
    "📅 Дневной анализ",
    "🏠 Статистика домохозяйств",
    "📈 Недельный/Месячный анализ",
    "⚠️ Аномалии"
])

# TAB 1: Обзор
with tab1:
    st.header("Общая статистика")
    
    # Запрос для общей статистики - показываем все данные если не выбран фильтр
    if show_all_data or date_range is None:
        overview_query = """
            SELECT 
                SUM(household_count) as total_households,
                SUM(total_consumption) as total_energy,
                AVG(avg_consumption) as avg_consumption,
                MAX(max_consumption) as peak_consumption
            FROM analytics.daily_consumption
        """
        overview_df = load_data(overview_query)
    elif len(date_range) == 2:
        start_date, end_date = date_range
        overview_query = """
            SELECT 
                SUM(household_count) as total_households,
                SUM(total_consumption) as total_energy,
                AVG(avg_consumption) as avg_consumption,
                MAX(max_consumption) as peak_consumption
            FROM analytics.daily_consumption
            WHERE day BETWEEN %(start_date)s AND %(end_date)s
        """
        overview_df = load_data(overview_query, {'start_date': start_date, 'end_date': end_date})
    else:
        overview_df = None
        
        if overview_df is not None and not overview_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            total_households = overview_df['total_households'].iloc[0]
            total_energy = overview_df['total_energy'].iloc[0]
            avg_consumption = overview_df['avg_consumption'].iloc[0]
            peak_consumption = overview_df['peak_consumption'].iloc[0]
            
            with col1:
                if total_households is not None:
                    st.metric("Всего измерений", f"{int(total_households):,}")
                else:
                    st.metric("Всего измерений", "0")
            with col2:
                if total_energy is not None:
                    st.metric("Общее потребление", f"{total_energy:,.2f} кВтч")
                else:
                    st.metric("Общее потребление", "0 кВтч")
            with col3:
                if avg_consumption is not None:
                    st.metric("Среднее потребление", f"{avg_consumption:,.4f} кВтч")
                else:
                    st.metric("Среднее потребление", "0 кВтч")
            with col4:
                if peak_consumption is not None:
                    st.metric("Пиковое потребление", f"{peak_consumption:,.4f} кВтч")
                else:
                    st.metric("Пиковое потребление", "0 кВтч")
        else:
            st.warning("Нет данных за выбранный период. Попробуйте выбрать период с 01.01.2024 по 03.01.2024")
    
    # График временного ряда
    st.subheader("Временной ряд потребления")
    # Запрос для временного ряда
    if show_all_data or date_range is None:
        daily_query = """
            SELECT day, total_consumption, avg_consumption, max_consumption, min_consumption
            FROM analytics.daily_consumption
            ORDER BY day
        """
        daily_df = load_data(daily_query)
    elif len(date_range) == 2:
        start_date, end_date = date_range
        daily_query = """
            SELECT day, total_consumption, avg_consumption, max_consumption, min_consumption
            FROM analytics.daily_consumption
            WHERE day BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY day
        """
        daily_df = load_data(daily_query, {'start_date': start_date, 'end_date': end_date})
    else:
        daily_df = None
    
    if daily_df is not None and not daily_df.empty:
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=daily_df['day'],
            y=daily_df['total_consumption'],
            mode='lines+markers',
            name='Общее потребление',
            line=dict(color='#1f77b4', width=2)
        ))
        
        fig.add_trace(go.Scatter(
            x=daily_df['day'],
            y=daily_df['avg_consumption'],
            mode='lines+markers',
            name='Среднее потребление',
            line=dict(color='#ff7f0e', width=2),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title="Динамика потребления электроэнергии",
            xaxis_title="Дата",
            yaxis_title="Общее потребление (кВтч)",
            yaxis2=dict(
                title="Среднее потребление (кВтч)",
                overlaying='y',
                side='right'
            ),
            hovermode='x unified',
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Нет данных для отображения графика")

# TAB 2: Почасовой анализ
with tab2:
    st.header("Почасовой анализ потребления")
    
    if show_all_data or date_range is None:
        hourly_query = """
            SELECT hour, total_consumption, avg_consumption, max_consumption, min_consumption
            FROM analytics.hourly_consumption
            ORDER BY hour
            LIMIT 1000
        """
        hourly_df = load_data(hourly_query)
    elif len(date_range) == 2:
        start_date, end_date = date_range
        hourly_query = """
            SELECT hour, total_consumption, avg_consumption, max_consumption, min_consumption
            FROM analytics.hourly_consumption
            WHERE DATE(hour) BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY hour
            LIMIT 1000
        """
        hourly_df = load_data(hourly_query, {'start_date': start_date, 'end_date': end_date})
    else:
        hourly_df = None
    
    if hourly_df is not None and not hourly_df.empty:
        # Тепловая карта по часам
        st.subheader("Тепловая карта потребления по часам")
        hourly_df['hour_of_day'] = pd.to_datetime(hourly_df['hour']).dt.hour
        hourly_df['day'] = pd.to_datetime(hourly_df['hour']).dt.date
        
        pivot_df = hourly_df.pivot_table(
            values='avg_consumption',
            index='day',
            columns='hour_of_day',
            aggfunc='mean'
        )
        
        if not pivot_df.empty:
            fig = px.imshow(
                pivot_df,
                labels=dict(x="Час дня", y="Дата", color="Потребление (кВтч)"),
                aspect="auto",
                color_continuous_scale="Viridis"
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
        
        # Среднее потребление по часам
        st.subheader("Среднее потребление по часам дня")
        hourly_avg = hourly_df.groupby('hour_of_day')['avg_consumption'].mean().reset_index()
        
        fig = px.bar(
            hourly_avg,
            x='hour_of_day',
            y='avg_consumption',
            labels={'hour_of_day': 'Час дня', 'avg_consumption': 'Среднее потребление (кВтч)'},
            color='avg_consumption',
            color_continuous_scale="Blues"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Нет почасовых данных")

# TAB 3: Дневной анализ
with tab3:
    st.header("Дневной анализ")
    
    if show_all_data or date_range is None:
        daily_query = """
            SELECT day, total_consumption, avg_consumption, max_consumption, min_consumption, household_count
            FROM analytics.daily_consumption
            ORDER BY day
        """
        daily_df = load_data(daily_query)
    elif len(date_range) == 2:
        start_date, end_date = date_range
        daily_query = """
            SELECT day, total_consumption, avg_consumption, max_consumption, min_consumption, household_count
            FROM analytics.daily_consumption
            WHERE day BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY day
        """
        daily_df = load_data(daily_query, {'start_date': start_date, 'end_date': end_date})
    else:
        daily_df = None
    
    if daily_df is not None and not daily_df.empty:
            # График с диапазоном (min-max)
            st.subheader("Диапазон потребления по дням")
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=daily_df['day'],
                y=daily_df['max_consumption'],
                mode='lines+markers',
                name='Максимум',
                line=dict(color='red', width=1),
                fill=None
            ))
            
            fig.add_trace(go.Scatter(
                x=daily_df['day'],
                y=daily_df['min_consumption'],
                mode='lines+markers',
                name='Минимум',
                line=dict(color='blue', width=1),
                fill='tonexty',
                fillcolor='rgba(0,100,200,0.2)'
            ))
            
            fig.add_trace(go.Scatter(
                x=daily_df['day'],
                y=daily_df['avg_consumption'],
                mode='lines+markers',
                name='Среднее',
                line=dict(color='green', width=2)
            ))
            
            fig.update_layout(
                title="Диапазон и среднее потребление",
                xaxis_title="Дата",
                yaxis_title="Потребление (кВтч)",
                hovermode='x unified',
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Статистика по дням недели
            st.subheader("Потребление по дням недели")
            daily_df['day_of_week'] = pd.to_datetime(daily_df['day']).dt.day_name()
            weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            daily_df['day_of_week'] = pd.Categorical(daily_df['day_of_week'], categories=weekday_order, ordered=True)
            
            weekday_stats = daily_df.groupby('day_of_week')['avg_consumption'].mean().reset_index()
            
            fig = px.bar(
                weekday_stats,
                x='day_of_week',
                y='avg_consumption',
                labels={'day_of_week': 'День недели', 'avg_consumption': 'Среднее потребление (кВтч)'},
                color='avg_consumption',
                color_continuous_scale="Greens"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Таблица с данными
            st.subheader("Данные по дням")
            st.dataframe(daily_df[['day', 'total_consumption', 'avg_consumption', 'max_consumption', 'min_consumption', 'household_count']], use_container_width=True)
    else:
        st.info("Нет данных за выбранный период")

# TAB 4: Статистика домохозяйств
with tab4:
    st.header("Статистика по домохозяйствам")
    
    household_query = """
        SELECT lclid, total_consumption, avg_daily_consumption, max_daily_consumption, min_daily_consumption, days_count
        FROM analytics.household_stats
        ORDER BY total_consumption DESC
        LIMIT 50
    """
    
    household_df = load_data(household_query)
    
    if household_df is not None and not household_df.empty:
        # Топ-10 домохозяйств
        st.subheader("Топ-10 домохозяйств по потреблению")
        top_10 = household_df.head(10)
        
        fig = px.bar(
            top_10,
            x='lclid',
            y='total_consumption',
            labels={'lclid': 'ID домохозяйства', 'total_consumption': 'Общее потребление (кВтч)'},
            color='total_consumption',
            color_continuous_scale="Reds"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Распределение потребления
        st.subheader("Распределение среднего дневного потребления")
        
        fig = px.histogram(
            household_df,
            x='avg_daily_consumption',
            nbins=20,
            labels={'avg_daily_consumption': 'Среднее дневное потребление (кВтч)', 'count': 'Количество домохозяйств'},
            color_discrete_sequence=['#2E86AB']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Таблица со статистикой
        st.subheader("Детальная статистика")
        st.dataframe(household_df, use_container_width=True)
    else:
        st.info("Нет данных по домохозяйствам")

# TAB 5: Недельный и месячный анализ
with tab5:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📅 Недельный анализ")
        
        if show_all_data or date_range is None:
            weekly_query = """
                SELECT week_start, total_consumption, avg_consumption, max_consumption, min_consumption
                FROM analytics.weekly_consumption
                ORDER BY week_start
            """
            weekly_df = load_data(weekly_query)
        elif len(date_range) == 2:
            start_date, end_date = date_range
            weekly_query = """
                SELECT week_start, total_consumption, avg_consumption, max_consumption, min_consumption
                FROM analytics.weekly_consumption
                WHERE week_start BETWEEN %(start_date)s AND %(end_date)s
                ORDER BY week_start
            """
            weekly_df = load_data(weekly_query, {'start_date': start_date, 'end_date': end_date})
        else:
            weekly_df = None
            
            if weekly_df is not None and not weekly_df.empty:
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=weekly_df['week_start'],
                    y=weekly_df['total_consumption'],
                    mode='lines+markers',
                    name='Общее потребление',
                    line=dict(color='#1f77b4', width=3)
                ))
                
                fig.add_trace(go.Scatter(
                    x=weekly_df['week_start'],
                    y=weekly_df['avg_consumption'],
                    mode='lines+markers',
                    name='Среднее потребление',
                    line=dict(color='#ff7f0e', width=2)
                ))
                
                fig.update_layout(
                    title="Недельное потребление",
                    xaxis_title="Неделя",
                    yaxis_title="Потребление (кВтч)",
                    height=400,
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нет недельных данных за выбранный период")
    
    with col2:
        st.subheader("📆 Месячный анализ")
        
        if show_all_data or date_range is None:
            monthly_query = """
                SELECT month, total_consumption, avg_consumption, max_consumption, min_consumption
                FROM analytics.monthly_consumption
                ORDER BY month
            """
            monthly_df = load_data(monthly_query)
        elif len(date_range) == 2:
            start_date, end_date = date_range
            monthly_query = """
                SELECT month, total_consumption, avg_consumption, max_consumption, min_consumption
                FROM analytics.monthly_consumption
                WHERE month BETWEEN %(start_date)s AND %(end_date)s
                ORDER BY month
            """
            monthly_df = load_data(monthly_query, {'start_date': start_date, 'end_date': end_date})
        else:
            monthly_df = None
        
        if monthly_df is not None and not monthly_df.empty:
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                x=monthly_df['month'],
                y=monthly_df['total_consumption'],
                name='Общее потребление',
                marker_color='#2E86AB'
            ))
            
            fig.update_layout(
                title="Месячное потребление",
                xaxis_title="Месяц",
                yaxis_title="Потребление (кВтч)",
                height=400,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Box plot для среднего потребления
            st.subheader("Распределение среднего потребления по месяцам")
            fig2 = px.box(
                monthly_df,
                y='avg_consumption',
                labels={'avg_consumption': 'Среднее потребление (кВтч)'},
                color_discrete_sequence=['#A23B72']
            )
            fig2.update_layout(height=300)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Нет месячных данных за выбранный период")

# TAB 6: Аномалии
with tab6:
    st.header("⚠️ Обнаруженные аномалии")
    
    if show_all_data or date_range is None:
        anomalies_query = """
            SELECT lclid, timestamp, energy_kwh, z_score, anomaly_type
            FROM analytics.anomalies
            ORDER BY timestamp DESC
            LIMIT 1000
        """
        anomalies_df = load_data(anomalies_query)
    elif len(date_range) == 2:
        start_date, end_date = date_range
        anomalies_query = """
            SELECT lclid, timestamp, energy_kwh, z_score, anomaly_type
            FROM analytics.anomalies
            WHERE DATE(timestamp) BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY timestamp DESC
            LIMIT 1000
        """
        anomalies_df = load_data(anomalies_query, {'start_date': start_date, 'end_date': end_date})
    else:
        anomalies_df = None
    
    if anomalies_df is not None and not anomalies_df.empty:
        # Статистика по аномалиям
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Всего аномалий", len(anomalies_df))
        
        with col2:
            anomaly_types = anomalies_df['anomaly_type'].value_counts()
            st.metric("Типов аномалий", len(anomaly_types))
        
        with col3:
            avg_z_score = anomalies_df['z_score'].abs().mean()
            st.metric("Средний |Z-score|", f"{avg_z_score:.2f}")
        
        # График аномалий по времени
        st.subheader("Временной ряд аномалий")
        
        anomalies_df['date'] = pd.to_datetime(anomalies_df['timestamp']).dt.date
        anomalies_by_date = anomalies_df.groupby('date').size().reset_index(name='count')
        
        fig = px.line(
            anomalies_by_date,
            x='date',
            y='count',
            labels={'date': 'Дата', 'count': 'Количество аномалий'},
            markers=True,
            color_discrete_sequence=['#FF6B6B']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Распределение по типам
        st.subheader("Распределение по типам аномалий")
        anomaly_type_counts = anomalies_df['anomaly_type'].value_counts().reset_index()
        anomaly_type_counts.columns = ['Тип аномалии', 'Количество']
        
        fig = px.pie(
            anomaly_type_counts,
            values='Количество',
            names='Тип аномалии',
            color_discrete_sequence=px.colors.sequential.Redor
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Scatter plot: Z-score vs энергия
        st.subheader("Z-score vs Потребление")
        fig = px.scatter(
            anomalies_df,
            x='energy_kwh',
            y='z_score',
            color='anomaly_type',
            hover_data=['lclid', 'timestamp'],
            labels={'energy_kwh': 'Потребление (кВтч)', 'z_score': 'Z-score'},
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Таблица с аномалиями
        st.subheader("Детальный список аномалий")
        st.dataframe(
            anomalies_df[['timestamp', 'lclid', 'energy_kwh', 'z_score', 'anomaly_type']],
            use_container_width=True
        )
    else:
        st.info("Нет аномалий")

# Футер
st.markdown("---")
st.markdown("**Энергетическая аналитика** | Разработано для проекта по обработке больших данных")
st.markdown("*Данные обновляются после выполнения ETL пайплайна в Airflow*")
