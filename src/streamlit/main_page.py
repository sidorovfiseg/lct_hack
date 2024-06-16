import streamlit as st
import pandas as pd
import numpy as np 
import requests
from io import StringIO
import plotly.express as px
import matplotlib.pyplot as plt
import json

# Функция для обработки данных
def process_data(response_data):
    # Парсинг JSON данных
    data = json.loads(response_data)

    # Разделение данных
    patent_counts = {k: v for k, v in data.items() if not isinstance(v, dict)}
    okopf_data = data.get("Разметка по ОКОПФ", [])
    msp_data = data.get("МСП", {})
    organizations_data = data.get("Организации", {})

    return patent_counts, okopf_data, msp_data, organizations_data



# Функция для построения графиков
def plot_data(patent_counts, okopf_data, msp_data, organizations_data):
    st.header("Общая информация по патентам")
    st.json(patent_counts)

    st.header("Разметка по ОКОПФ")
    okopf_df = pd.DataFrame(okopf_data)
    st.dataframe(okopf_df)

    st.header("МСП")
    st.json(msp_data)
    
    st.header("Организации")
    st.json(organizations_data)

    # Построение круговой диаграммы по ОКОПФ
    if not okopf_df.empty:
        fig_okopf = px.pie(okopf_df, names='ОКОПФ (расшифровка)', values='count', title='Разметка по ОКОПФ')
        st.plotly_chart(fig_okopf)

    # Построение круговой диаграммы по категориям МСП
    if "По категории субъекта" in msp_data:
        msp_category_df = pd.DataFrame(msp_data["По категории субъекта"])
        if not msp_category_df.empty:
            fig_msp_category = px.pie(msp_category_df, names='Категория субъекта', values='count', title='МСП по категориям субъекта')
            st.plotly_chart(fig_msp_category)

    # Построение круговой диаграммы по видам предпринимательства МСП
    if "По виду предпринимательства" in msp_data:
        msp_type_df = pd.DataFrame(msp_data["По виду предпринимательства"])
        if not msp_type_df.empty:
            fig_msp_type = px.pie(msp_type_df, names='Вид предпринимательства', values='count', title='МСП по видам предпринимательства')
            st.plotly_chart(fig_msp_type)


def upload_file(file):
    if file is not None:
        files = {'file': file.getvalue()}
        response = requests.post('https://climbing-fox-open.ngrok-free.app/lct_hack/doc_markup', files=files)
        return response
    return None

def get_dashboard_data(file):
    if file is not None:
        files = {'file': file.getvalue()}
        response = requests.post('https://climbing-fox-open.ngrok-free.app/lct_hack/doc_dashboard', files=files)
        if response.status_code == 200:
            return response.json()
    return None


st.title("Загрузка и отображение документа")

# Загрузка файла
uploaded_file = st.file_uploader("Загрузите файл", type=['csv'], help="Пожалуйста, загрузите файл в формате CSV.")

# Кнопка для загрузки документа
if st.button('Загрузить документ', disabled=uploaded_file is None):
    if uploaded_file is not None:
        # Чтение содержимого загруженного файла
        cached_file = StringIO(uploaded_file.getvalue().decode("utf-8"))
        
        # Отправляем запрос на сервер
        response = upload_file(cached_file)
        
        # Проверяем ответ от сервера
        if response is not None and response.status_code == 200:
            st.success('Документ успешно загружен.')

            # Чтение CSV контента из ответа
            csv_content = response.text
            data = pd.read_csv(StringIO(csv_content))
            
            # Форматирование колонок
            if 'ИНН' in data.columns:
                data['ИНН'] = data['ИНН'].apply(lambda x: f'{int(x):d}')

            if 'registration number' in data.columns:
                data['registration number'] = data['registration number'].apply(lambda x: f'{int(x):d}' if pd.notnull(x) else '')

            # Отображение данных в виде таблицы без индекса
            st.header("Результат:", anchor=False)
            st.dataframe(data, hide_index=True)
            
            # Получение данных с второго эндпоинта
            dashboard_data = get_dashboard_data(cached_file)
            if dashboard_data:
                st.header("Статистика:", anchor=False)
                
                # Построение графиков по данным из ОКОПФ
                okopf_data = pd.DataFrame(dashboard_data['Разметка по ОКОПФ'])
                okopf_data["ОКОПФ (расшифровка)"] = okopf_data["ОКОПФ (расшифровка)"].fillna("")
                okopf_data["labels"] = okopf_data.apply(lambda row: f"{row['ОКОПФ (код)']} - {row['ОКОПФ (расшифровка)']}" if row['ОКОПФ (расшифровка)'] else row['ОКОПФ (код)'], axis=1)

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
        else:
            st.error('Ошибка при загрузке документа')
    else:
        st.warning('Пожалуйста, загрузите файл перед отправкой.')
        

if st.button('Получить данные с базы'):
    # Отправка GET запроса на сервер
    response = requests.get('https://climbing-fox-open.ngrok-free.app/lct_hack/db_dashboard')
    
    # Проверка ответа
    if response.status_code == 200:
        data = response.json()
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
            fig2 = px.bar(df_okopf, x="ОКОПФ (код)", y="count", title='Разметка по ОКОПФ', labels={'count':'Count', 'ОКОПФ (код)':'ОКОПФ'})

            st.plotly_chart(fig1)
            st.plotly_chart(fig2)
    else:
        st.error('Ошибка при получении данных с сервера')

    

    
    
