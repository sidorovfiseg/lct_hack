import requests
import streamlit as st

@st.cache_data
def get_doc_markup(file, limit=100):
    if file is not None:
        files = {'file': ('uploaded_file.csv', file.getvalue().encode('utf-8'), 'text/csv')}
        params = {'limit': limit}
        response = requests.post('https://climbing-fox-open.ngrok-free.app/lct_hack/doc_markup_new', files=files, params=params)
        return response
    return None

@st.cache_data
def get_doc_dashboard(file):
    if file is not None:
        files = {'file': ('uploaded_file.csv', file, 'text/csv')}
        response = requests.post('https://climbing-fox-open.ngrok-free.app/lct_hack/doc_dashboard', files=files)
        if response.status_code == 200:
            return response.json()
    return None

@st.cache_data   
def get_db_dashboard():
    response = requests.get('https://climbing-fox-open.ngrok-free.app/lct_hack/db_dashboard')
    if response.status_code == 200:
        return response.json()
    return None


@st.cache_data
def get_clusters(file):
    if file is not None:
        print(file)
        files = {'file': ('uploaded_file.csv', file.encode('utf-8'), 'text/csv')}
        response = requests.post('https://climbing-fox-open.ngrok-free.app/lct_hack/clusters', files=files)
        print(response)
        if response.status_code == 200:
            return response.json()
    return None