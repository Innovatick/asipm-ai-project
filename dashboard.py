# dashboard.py
# Версия: 2.1 (Стабильная, с исправлением KeyError и унификацией)

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from typing import Tuple, Optional

# --- НАСТРОЙКИ СТРАНИЦЫ ---
st.set_page_config(
    page_title="ASIPM-AI: Центр Управления Полетами",
    page_icon="🚀",
    layout="wide"
)

# --- ФУНКЦИИ ДЛЯ ЗАГРУЗКИ И ОБРАБОТКИ ДАННЫХ ---
@st.cache_data(ttl=300)
def load_data_from_gsheets() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    try:
        creds = Credentials.from_service_account_file('credentials.json', scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBYS_DhGNsTo-Dnph3g_H27aHQOoY0EOcmCIKarb7Zc/")
        
        analysis_df = pd.DataFrame(spreadsheet.worksheet('Analysis').get_all_records())
        holdings_df = pd.DataFrame(spreadsheet.worksheet('Holdings').get_all_records())
        history_df = pd.DataFrame(spreadsheet.worksheet('History_OHLCV').get_all_records())
        
        return analysis_df, holdings_df, history_df
    except Exception as e:
        st.error(f"Ошибка загрузки данных из Google Sheets: {e}")
        return None, None, None

def preprocess_data(analysis_df, holdings_df, history_df):
    if 'Watch' in holdings_df.columns:
        holdings_df['Watch'] = holdings_df['Watch'].astype(str).str.upper()
    
    analysis_df['Last_Update_DT'] = pd.to_datetime(analysis_df['Last_Update'], format="%d.%m.%Y %H:%M:%S", errors='coerce')
    
    for col in ['RSI_14', 'MA_20', 'MA_50', 'BB_Upper', 'BB_Lower']:
        if col in analysis_df.columns:
            clean_series = analysis_df[col].astype(str).str.replace(' ', '').str.replace(',', '.')
            analysis_df[col] = pd.to_numeric(clean_series, errors='coerce')
            
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if col in history_df.columns:
            clean_series = history_df[col].astype(str).str.replace(' ', '').str.replace(',', '.')
            history_df[col] = pd.to_numeric(clean_series, errors='coerce')
            
    if 'Date' in history_df.columns:
        history_df['Date'] = pd.to_datetime(history_df['Date'], errors='coerce')
            
    return analysis_df, holdings_df, history_df

# --- ОСНОВНОЙ ИНТЕРФЕЙС ДАШБОРДА ---
st.title("🚀 ASIPM-AI: Центр Управления Полетами")

analysis_raw, holdings_raw, history_raw = load_data_from_gsheets()

if analysis_raw is None or holdings_raw is None or history_raw is None:
    st.error("Не удалось загрузить данные. Проверьте подключение и права доступа.")
    st.stop()

analysis_df, holdings_df, history_df = preprocess_data(analysis_raw, holdings_raw, history_raw)

# --- ЛЕВЫЙ САЙДБАР ---
with st.sidebar:
    st.header("Состояние системы")
    last_update_time = analysis_df['Last_Update_DT'].max()
    if pd.notna(last_update_time):
        st.metric("Последнее обновление", last_update_time.strftime("%Y-%m-%d %H:%M:%S"))
    else:
        st.warning("Нет данных о времени обновления.")
    
    st.metric("Актив на мониторинге", holdings_df[holdings_df['Watch'] == 'TRUE'].shape[0])
    
    attention_states = ['Oversold', 'Warning', 'Proximity']
    st.metric("В 'Зоне внимания'", analysis_df[analysis_df['State'].isin(attention_states)].shape[0])

    st.header("Hotlist Intraday")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Strategic")
        st.dataframe(holdings_df[(holdings_df['Priority'] == 'Strategic') & (holdings_df['Watch'] == 'TRUE')][['Ticker']], hide_index=True, use_container_width=True)
    with col2:
        st.subheader("Promising")
        st.dataframe(holdings_df[(holdings_df['Priority'] == 'Promising') & (holdings_df['Watch'] == 'TRUE')][['Ticker']], hide_index=True, use_container_width=True)

# --- ОСНОВНАЯ ОБЛАСТЬ ---
col_main, col_right = st.columns([3, 1])

with col_main:
    st.subheader("Стратегический обзор")
    strategic_list = holdings_df[holdings_df['Priority'] == 'Strategic']['Ticker'].tolist()
    strategic_df = analysis_df[analysis_df['Ticker'].isin(strategic_list) & (analysis_df['Timeframe'] == 'D1')].copy()
    if not strategic_df.empty:
        sparklines = [history_df[history_df['Ticker'] == ticker].nlargest(10, 'Date')['Close'].tolist() for ticker in strategic_df['Ticker']]
        strategic_df['Тренд (10д)'] = sparklines
        st.dataframe(strategic_df[['Ticker', 'State', 'RSI_14', 'MA_20', 'MA_50', 'Тренд (10д)']], column_config={"Тренд (10д)": st.column_config.LineChartColumn(width="small")}, hide_index=True, use_container_width=True)

    st.subheader("Тактический контроль")
    tactical_df = analysis_df[analysis_df['State'].isin(attention_states) & (analysis_df['Timeframe'] == 'D1')].copy()
    if not tactical_df.empty:
        st.dataframe(tactical_df[['Ticker', 'State', 'RSI_14', 'MA_20', 'MA_50']], hide_index=True, use_container_width=True)
    else:
        st.info("Тактических сигналов нет.")

with col_right:
    st.subheader("Валютный монитор")
    currency_tickers = ['USD/RUB', 'EUR/RUB', 'CNY/RUB', 'USD000UTSTOM', 'EUR_RUB__TOM']
    currency_df = analysis_df[analysis_df['Ticker'].isin(currency_tickers) & (analysis_df['Timeframe'] == 'D1')]
    if not currency_df.empty:
        # ИСПРАВЛЕНО: Убран несуществующий столбец 'Close'
        st.dataframe(currency_df[['Ticker', 'State', 'RSI_14', 'MA_50']], hide_index=True, use_container_width=True)
    else:
        st.info("Нет данных по валютам.")