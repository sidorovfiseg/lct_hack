import streamlit as st
import pandas as pd
from io import StringIO
import json
import plotly.express as px
from utils.data_processing import process_data, format_data
from utils.file_upload import get_doc_markup, get_doc_dashboard, get_db_dashboard, get_clusters
from streamlit_agraph import agraph, Node, Edge, Config

st.title("Загрузка и отображение документа")

uploaded_file = st.file_uploader('Загрузите файл', type=['csv'], help='Пожалуйста, загрузите файл в формате CSV.')

if st.button('Загрузить документ', disabled=uploaded_file is None):
    if uploaded_file is not None:
        
        file_content = StringIO(uploaded_file.getvalue().decode("utf-8"))
        
        response = get_doc_markup(file_content)
        
        if response is not None and response.status_code == 200:
            st.success('Документ успешно загружен.')
            
            csv_content = response.content.decode('utf-8')
            data = pd.read_csv(StringIO(csv_content))
            
            data = format_data(data)
            
            # Отображение данных в виде таблицы без индекса
            st.header("Результат:", anchor=False)
            st.dataframe(data, hide_index=True)
            
            
            # Получение данных с второго эндпоинта
            dashboard_data = get_doc_dashboard(file_content)

            if dashboard_data:
                st.header('Статистка', anchor=False)
                
                # Построение графиков по данным из ОКОПФ
                okopf_data = pd.DataFrame(dashboard_data['Разметка по ОКОПФ'])
                okopf_data["ОКОПФ (расшифровка)"] = okopf_data["ОКОПФ (расшифровка)"].fillna("")
                okopf_data["labels"] = okopf_data.apply(lambda row: f"{row['ОКОПФ (код)']} - {row['ОКОПФ (расшифровка)']}" if row['ОКОПФ (расшифровка)'] else row['ОКОПФ (код)'], axis=1)
                okopf_data = okopf_data.sort_values(by="count", ascending=False)
                
                # Построение бар графика
                # Построение бар графика
                fig = px.bar(okopf_data, x='labels', y='count', title='Количество по ОКОПФ', text='count')
                fig.update_layout(
                    xaxis_title='ОКОПФ',
                    yaxis_title='Количество',
                    xaxis_tickangle=-45,
                    height=800
                )
                fig.update_traces(
                    hovertemplate='<b>%{x}</b><br>Количество: <b>%{y}</b><extra></extra>'
                )
                st.plotly_chart(fig)

                # Дополнительные графики на основе других данных
                st.subheader("Количество по категориям")
                st.metric("Количество размеченных изобретений", dashboard_data["Количество размеченных изобретений"])
                st.metric("Количество размеченных промышленных образцов", dashboard_data["Количество размеченных промышленных образцов"])
                st.metric("Количество размеченных полезных моделей", dashboard_data["Количество размеченных полезных моделей"])
                st.metric("Количество размеченных организаций", dashboard_data["Количество размеченных организаций"])
                
                # Построение графиков для МСП
                msp_data = pd.DataFrame(dashboard_data['МСП']['По категории субъекта'])
                fig_msp = px.pie(msp_data, values='count', names='Категория субъекта', title='МСП по категории субъекта')
                st.plotly_chart(fig_msp)

                # Построение графиков для вида предпринимательства
                entrepreneurship_data = pd.DataFrame(dashboard_data['МСП']['По виду предпринимательства'])
                fig_entrepreneurship = px.pie(entrepreneurship_data, values='count', names='Вид предпринимательства', title='МСП по виду предпринимательства')
                st.plotly_chart(fig_entrepreneurship)
            else:
                st.error('Ошибка при получении данных с второго эндпоинта')
                
            clusters_response = get_clusters(csv_content)
            
            if clusters_response:
                st.success("Кластеры успешно получены")
                clusters = clusters_response  # JSON data
                for cluster, patents in clusters.items():
                    st.write(f'## Кластер: {cluster}')
                    st.write('### Патенты:')
                    df = pd.DataFrame(patents, columns=['Патенты'])
                    st.write(df)
                    st.write('---')              
            else:
                st.error('Ошибка при получении данных с третьего эндпоинта')   
        else:
            st.error('Ошибка при загрузке документа')
    else:
        st.warning('Пожалуйста, загрузите файл перед отправкой.')

if st.button('Получить данные с базы'):
    data = get_db_dashboard()
    if data:
        st.write("Количество изобретений:", data["Количество изобретений"])
        st.write("Количество промышленных образцов:", data["Количество промышленных образцов"])
        st.write("Количество полезных моделей:", data["Количество полезных моделей"])
        st.write("Количество размеченных изобретений:", data["Количество размеченных изобретений"])
        st.write("Количество размеченных промышленных образцов:", data["Количество размеченных промышленных образцов"])
        st.write("Количество размеченных полезных моделей:", data["Количество размеченных полезных моделей"])
        st.write("Количество размеченных организаций:", data["Количество размеченных организаций"])

        # Построение круговой диаграммы
        labels = ['Изобретения', 'Промышленные образцы', 'Полезные модели']
        sizes = [data["Количество изобретений"], data["Количество промышленных образцов"], data["Количество полезных моделей"]]
        fig1 = px.pie(names=labels, values=sizes, title='Распределение по типу')

        # Построение столбчатой диаграммы
        df_okopf = pd.DataFrame(data["Разметка по ОКОПФ"])
        df_okopf_sorted = df_okopf.sort_values(by="count", ascending=False)
        
        fig2 = px.bar(df_okopf_sorted, x="ОКОПФ (код)", y="count", title='Разметка по ОКОПФ', labels={'count':'Count', 'ОКОПФ (код)':'ОКОПФ'})

        st.plotly_chart(fig1)
        st.plotly_chart(fig2)
    else:
        st.error('Ошибка при получении данных с сервера')