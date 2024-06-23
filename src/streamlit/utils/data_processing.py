import json 
import pandas as pd


def process_data(response_data):
    data = json.loads(response_data)
    
    patent_counts = {k: v for k, v in data.items() if not isinstance(v, dict)}
    okopf_data = data.get("Разметка по ОКОПФ", [])
    msp_data = data.get("МСП", {})
    organizations_data = data.get("Организация", {})
    
    return patent_counts, okopf_data, msp_data, organizations_data


def format_data(data):
    # Форматирование колонок
    if 'ИНН' in data.columns:
        data['ИНН'] = data['ИНН'].apply(lambda x: f'{int(x):d}')

    if 'ID компании' in data.columns:
        data['ID компании'] = data['ID компании'].apply(lambda x: f'{int(x):d}' if pd.notnull(x) else '')

    if 'registration_number' in data.columns:
        data['registration_number'] = data['registration_number'].apply(lambda x: f'{int(x):d}' if pd.notnull(x) else '')

    return data