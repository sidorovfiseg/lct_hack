import plotly.express as px
import streamlit as st
import pandas as pd

def plot_data(patent_counts, okopf_data, msp_data, organization_data):
    st.header("Общая информация по патентам")
    st.json(patent_counts)
    
    st.header("Разметка по ОКОПФ")
    okopf_df = pd.DataFrame(okopf_data)
    st.dataframe(okopf_data)
    
    st.header("МСП")
    st.json(msp_data)
    
    st.header("Организация")
    st.json(organization_data)
    
    # Построение круговой диаграммы по ОКОПФ
    if not okopf_df.empty:
        fig_okopf = px.pie(okopf_df, names='ОКОПФ (расшифровка)', values='count', title='Разметка по ОКОПФ')
        st.plotly_chart(fig_okopf)
        
    if "По категории субъекта" in msp_data:
        msp_category_df = pd.DataFrame(msp_data['По категории субъекта'])
        if not msp_category_df.empty:
            fig_msp_category = px.pie(msp_category_df, names='Категория субъекта', values='count', title='МСП по категориям субъекта')
            st.plotly_chart(fig_msp_category)
            
    if 'По виду предпринимательства' in msp_data:
        msp_type_df = pd.DataFrame(msp_data['По виду предпринимательства'])
        if not msp_type_df.empty:
            fig_msp_type = px.pie(msp_type_df, names='Вид предпринимательства', values='count', title='МСП по видам предпринимательства')
            st.plotly_chart(fig_msp_type)