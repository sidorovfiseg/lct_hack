import asyncio
import datetime
import io
import logging
import time
import uuid
from collections import defaultdict
from typing import Dict, Any, Optional, List

import pandas as pd
from aiohttp import ClientSession
from asyncpg import Connection
from fastapi import APIRouter, Depends, UploadFile, File
from langchain_community.chat_models.gigachat import GigaChat
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel
from sklearn.cluster import DBSCAN
from starlette.responses import StreamingResponse

from common.api.dependencies import get_client_session, get_db_connection
from common.db.model import get_inventions_by_inn, get_company_patents_by_inns, get_invention_count, get_industrial_design_count, get_utility_model_count, get_okopf_count, get_marked_patent_count, get_patent_counts_by_inns, get_organisatons_with_patents_count_by_inns, get_okopf_count_by_inns, get_msp_count_by_inns, get_org_count_by_inns, get_msp_classification_category_by_inns, get_msp_classification_type_by_inns, get_org_classification_by_inns, insert_patent_holder, get_full_names_by_inns
from common.domain.schema import TestRequest, SearchRequest, SearchTextRequest
from common.utils.debug import async_timer
from lct_app.embeddings.giga_chat_setting import giga_chat_api_config
from lct_app.embeddings.main import GigaChatEmbeddingFunction

lcthack_router = APIRouter(
    prefix="/lct_hack",
    tags=["Lct Hack"],
)


@lcthack_router.post(
    "/hello",
    response_model_exclude_none=True,
)
@async_timer
async def search(
        query: TestRequest = Depends(),
        session: ClientSession = Depends(get_client_session),
        db: Connection = Depends(get_db_connection),
):
    test_res = await get_inventions_by_inn(db, '9729275828')
    logging.info(f"{test_res=}")

    return test_res


@lcthack_router.post(
    "/doc_markup",
    response_model_exclude_none=True,
)
@async_timer
async def markup(
        file: UploadFile = File(...),
        session: ClientSession = Depends(get_client_session),
        db: Connection = Depends(get_db_connection),
):
    contents = await file.read()

    file_extension = file.filename.split('.')[-1]

    if file_extension == 'csv':
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')), on_bad_lines='skip')
    elif file_extension in ['xls', 'xlsx']:
        df = pd.read_excel(io.BytesIO(contents))
    else:
        return {"error": "Unsupported file type"}

    inns = df['ИНН'].unique().tolist()
    inns = [int(inn) for inn in inns]
    results = await get_company_patents_by_inns(db, inns)

    results_df = pd.DataFrame(results)
    merged_df = pd.merge(df, results_df, on='ИНН', how='left')

    output = io.BytesIO()
    if file_extension == 'csv':
        merged_df.to_csv(output, index=False)
        output.seek(0)
        return StreamingResponse(output, headers={"Content-Disposition": "attachment; filename=result.csv"})  # , media_type="text/csv"
    elif file_extension in ['xls', 'xlsx']:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            merged_df.to_excel(writer, index=False, sheet_name='Sheet1')
        output.seek(0)
        return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=result.xlsx"})


@lcthack_router.post(
    "/inn_names",
    response_model_exclude_none=True,
)
@async_timer
async def inn_names(
        file: UploadFile = File(...),
        session: ClientSession = Depends(get_client_session),
        db: Connection = Depends(get_db_connection),
):
    contents = await file.read()

    file_extension = file.filename.split('.')[-1]

    if file_extension == 'csv':
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')), on_bad_lines='skip')
    elif file_extension in ['xls', 'xlsx']:
        df = pd.read_excel(io.BytesIO(contents))
    else:
        return {"error": "Unsupported file type"}

    inns = df['ИНН'].unique().tolist()

    tasks = [get_name_by_inn(inn, session) for inn in inns]
    results = await asyncio.gather(*tasks)

    api_results = {result["inn"]: result["name"] for result in results}

    db_results = await get_full_names_by_inns(db, inns)

    merged_result = defaultdict(set)

    for api_inn, api_name in api_results.items():
        if api_name:
            merged_result[api_inn].add(api_name.lower())
    for db_inn, db_name in db_results.items():
        if db_name:
            merged_result[db_inn].add(db_name.lower())

    return dict(merged_result)


@lcthack_router.post(
    "/doc_dashboard",
    response_model_exclude_none=True,
)
@async_timer
async def doc_dashboard(
        file: UploadFile = File(...),
        session: ClientSession = Depends(get_client_session),
        db: Connection = Depends(get_db_connection),
):
    contents = await file.read()
    file_extension = file.filename.split('.')[-1]

    if file_extension == 'csv':
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')), on_bad_lines='skip')
    elif file_extension in ['xls', 'xlsx']:
        df = pd.read_excel(io.BytesIO(contents))
    else:
        return {"error": "Unsupported file type"}

    inns = df['ИНН'].unique().tolist()

    marked_inv_count_by_inns, marked_ind_count_by_inns, marked_uti_count_by_inns = await get_patent_counts_by_inns(db, inns)

    org_with_pat = await get_organisatons_with_patents_count_by_inns(db, inns)
    okopf = await get_okopf_count_by_inns(db, inns)
    msp_count = await get_msp_count_by_inns(db, inns)
    org_count = await get_org_count_by_inns(db, inns)
    msp_cat_class_count = await get_msp_classification_category_by_inns(db, inns)
    msp_type_class_count = await get_msp_classification_type_by_inns(db, inns)
    org_class_count = await get_org_classification_by_inns(db, inns)

    return {
        "Количество размеченных изобретений": marked_inv_count_by_inns,
        "Количество размеченных промышленных образцов": marked_ind_count_by_inns,
        "Количество размеченных полезных моделей": marked_uti_count_by_inns,
        "Количество размеченных организаций": org_with_pat,
        "Разметка по ОКОПФ": okopf,
        "МСП": {
            "Общее количество": msp_count,
            "По категории субъекта": msp_cat_class_count,
            "По виду предпринимательства": msp_type_class_count
        },
        "Организации": {
            "Общее количество": org_count,
            "По типу объекта": org_class_count
        }
    }


@lcthack_router.get(
    "/db_dashboard",
    response_model_exclude_none=True,
)
@async_timer
async def db_dashboard(
        session: ClientSession = Depends(get_client_session),
        db: Connection = Depends(get_db_connection),
):
    inv_count = await get_invention_count(db)
    ind_count = await get_industrial_design_count(db)
    uti_count = await get_utility_model_count(db)
    marked_inv_count = await get_marked_patent_count(db, 'Изобретение')
    marked_ind_count = await get_marked_patent_count(db, 'Промышленный образец')
    marked_uti_count = await get_marked_patent_count(db, 'Полезная модель')
    org_with_pat = marked_inv_count + marked_ind_count + marked_uti_count
    okopf = await get_okopf_count(db)
    return {"Количество изобретений": inv_count,
            "Количество промышленных образцов": ind_count,
            "Количество полезных моделей": uti_count,
            "Количество размеченных изобретений": marked_inv_count,
            "Количество размеченных промышленных образцов": marked_ind_count,
            "Количество размеченных полезных моделей": marked_uti_count,
            "Количество размеченных организаций": org_with_pat,
            "Разметка по ОКОПФ": okopf}


embedder = GigaChatEmbeddingFunction(giga_chat_api_config.TOKEN, giga_chat_api_config.SCOPE)
llm = GigaChat(credentials=giga_chat_api_config.TOKEN, scope=giga_chat_api_config.SCOPE, verify_ssl_certs=False, model='GigaChat-Plus', profanity_check=None, verbose=False)


@lcthack_router.post(
    "/clusters",
    response_model_exclude_none=True,
)
@async_timer
async def get_clusters(
        file: UploadFile = File(...),
):
    try:
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))

        df['id_'] = [uuid.uuid4() for _ in range(len(df))]

        ids_to_patent_names = {row['id_']: row['patent_name'] for _, row in df.dropna(subset=['patent_name']).iterrows()}

        unique_patent_names = list(set(ids_to_patent_names.values()))

        embeddings = embedder(unique_patent_names)

        eps = 15
        min_samples = 2

        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
        labels = dbscan.fit_predict(embeddings)

        patent_name_to_cluster = {name: cluster for name, cluster in zip(unique_patent_names, labels)}

        df['Cluster'] = df['patent_name'].map(patent_name_to_cluster)

        # Grouping and sorting as per clusters
        cluster_group = df.groupby("Cluster")['patent_name'].apply(list).to_dict()

        cluster_titles = {}
        for cluster_id, patent_names in cluster_group.items():
            reviews = "\n".join(patent_names)

            messages = [
                SystemMessage(
                    content="Добро пожаловать в систему кластеризации патентных названий. Пожалуйста, укажите тему для следующих названий патентов.",
                ),
                HumanMessage(content="Что общего между следующими названиями патентов?\n\nНазвания патентов:\n\n" + reviews + "\n\nТема:"),
            ]

            response = llm.invoke(messages).content
            title = response.strip()
            cluster_titles[title] = patent_names

        return cluster_titles
    except ValueError:
        raise ValueError("Invalid file format. Please upload a CSV file with a 'patent_name' column.")


headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'DNT': '1',
    'Origin': 'https://egrul.nalog.ru',
    'Referer': 'https://egrul.nalog.ru/index.html',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}


@lcthack_router.post(
    "/search",
    response_model_exclude_none=True,
)
@async_timer
async def search_external_api(
        message: SearchRequest = Depends(),
        session: ClientSession = Depends(get_client_session),
        db: Connection = Depends(get_db_connection),
):
    print(f'{message=}')
    data = {
        'vyp3CaptchaToken': '',
        'page': '',
        'query': message.text,
        'region': '',
        'PreventChromeAutocomplete': '',
    }
    if message.strict:
        data['nameEq'] = 'on'

    async with session.post('https://egrul.nalog.ru/', headers=headers, data=data) as response:
        response_json = await response.json()
        print(response_json)

        t = response_json['t']

        async def get_search_results(session, t):
            while True:
                current_time_ms = int(time.time() * 1000)
                params = {
                    'r': str(current_time_ms),
                    '_': str(current_time_ms),
                }

                async with session.get(f'https://egrul.nalog.ru/search-result/{t}', params=params, headers=headers) as response:
                    response_json = await response.json()
                    if response_json.get("status") != "wait":
                        return response_json

                await asyncio.sleep(1)

        search_result = await get_search_results(session, t)
        rows = search_result.get('rows', None)
        if rows:
            patent_holder = message.text
            inn = rows[0].get('i', None)
            if not inn:
                return search_result
            inn = str(rows[0]['i'])
            await insert_patent_holder(db, patent_holder, inn)
            return rows[0]

        return search_result


class Patent(BaseModel):
    id: str
    title_ru: Optional[str] = None
    title_en: Optional[str] = None
    publication_date: Optional[datetime.date] = None
    application_number: Optional[str] = None
    application_filing_date: Optional[datetime.date] = None
    ipc: Optional[List[str]] = None
    cpc: Optional[List[str]] = None
    snippet_ru: Optional[str] = None
    snippet_en: Optional[str] = None
    abstract_ru: Optional[str] = None
    abstract_en: Optional[str] = None
    claims_ru: Optional[str] = None
    claims_en: Optional[str] = None
    description_ru: Optional[str] = None
    description_en: Optional[str] = None
    patentees_ru: Optional[List[str]] = None
    patentees_en: Optional[List[str]] = None
    applicants_ru: Optional[List[str]] = None
    applicants_en: Optional[List[str]] = None
    inventors_ru: Optional[List[str]] = None
    inventors_en: Optional[List[str]] = None
    similarity: Optional[float] = None
    similarity_norm: Optional[float] = None
    referred_from_ids: Optional[List[str]] = None
    prototype_docs_ids: Optional[List[str]] = None


async def fetch_rospatent_data(session, name, limit):
    name_ = name.replace('"', '\\"')
    headers = {
        'Accept': 'application/json',
    }
    json_data = {
        'q': f'PE="{name_}"',
        'offset': 0,
        'limit': limit,
        'pre_tag': '',
        'post_tag': '',
        'include_facets': 0,
        'sort': 'relevance',
        'datasets': [
            'ru_till_1994',
            'ru_since_1994',
            'cis',
            'dsgn_ru',
        ],
        'preffered_lang': 'ru',
    }
    results = []
    params = {
        't': int(time.time() * 1000),
    }
    async with session.post('https://searchplatform.rospatent.gov.ru/search', params=params, headers=headers, json=json_data) as response:
        response_json = await response.json(content_type=None)

        hits = response_json['hits']
        print(f'{len(hits)=}')
        for hit in hits:
            total = response_json['total']
            id_ = hit['id']
            snippet = hit['snippet']
            title = snippet['title']
            snipped_description = snippet['description']

            common = hit['common']
            application = common.get('application', {})
            application_number = application.get('number', None)
            application_filing_date = datetime.datetime.strptime(application['filing_date'], "%Y.%m.%d") if application.get('filing_date') else None

            publication_date = datetime.datetime.strptime(common['publication_date'], "%Y.%m.%d")

            classification = common.get('classification', {})
            ipc = classification.get('ipc', None)
            if ipc:
                ipc = [current['fullname'] for current in ipc if current.get('fullname')]

            cpc = classification.get('cpc', None)
            if cpc:
                cpc = [current['fullname'] for current in cpc if current.get('fullname')]

            biblio = hit['biblio']
            biblio_ru = biblio.get('ru', None)
            biblio_en = biblio.get('en', None)

            title_ru, title_en = None, None
            patentees_ru, applicants_ru, inventors_ru = None, None, None
            patentees_en, applicants_en, inventors_en = None, None, None

            if biblio_ru:
                title_ru = biblio_ru.get('title', None)
                patentee_ru = biblio_ru.get('patentee', None)
                if patentee_ru:
                    patentees_ru = [current['name'] for current in patentee_ru]

                applicant_ru = biblio_ru.get('applicant', None)
                if applicant_ru:
                    applicants_ru = [current['name'] for current in applicant_ru]

                inventor_ru = biblio_ru.get('inventor', None)
                if inventor_ru:
                    inventors_ru = [current['name'] for current in inventor_ru]

            if biblio_en:
                title_en = biblio_en.get('title', None)
                patentee_en = biblio_en.get('patentee', None)
                if patentee_en:
                    patentees_en = [current['name'] for current in patentee_en]

                applicant_en = biblio_en.get('applicant', None)
                if applicant_en:
                    applicants_en = [current['name'] for current in applicant_en]

                inventor_en = biblio_en.get('inventor', None)
                if inventor_en:
                    inventors_en = [current['name'] for current in inventor_en]

            results.append(
                Patent(
                    id=id_,
                    title_ru=title_ru,
                    title_en=title_en,
                    publication_date=publication_date,
                    application_number=application_number,
                    application_filing_date=application_filing_date,
                    ipc=ipc,
                    cpc=cpc,
                    snippet_ru=snipped_description,
                    patentees_ru=patentees_ru,
                    patentees_en=patentees_en,
                    applicants_ru=applicants_ru,
                    applicants_en=applicants_en,
                    inventors_ru=inventors_ru,
                    inventors_en=inventors_en,
                )
            )
    return {name: results}


@lcthack_router.post(
    "/rospatent_search",
    response_model_exclude_none=True,
)
@async_timer
async def rospatent_search(
        message: SearchTextRequest,
        session: ClientSession = Depends(get_client_session),

) -> Dict[str, List[Patent]]:
    return await fetch_rospatent_data(session, message.text)


async def get_name_by_inn(inn: int, session: ClientSession) -> Dict[str, Any]:
    try:
        async with session.post(f'https://egrul.itsoft.ru/{inn}.json', headers={'accept': 'application/json'}) as response:
            response_json = await response.json()
        if 'СвЮЛ' in response_json:
            legal_entity = response_json['СвЮЛ']
            legal_entity_name = legal_entity['СвНаимЮЛ']['@attributes']['НаимЮЛПолн']
            return {"inn": inn, "name": legal_entity_name}
        elif 'СвИП' in response_json:
            individual_entrepreneur = response_json['СвИП']
            individual_entrepreneur_attributes = individual_entrepreneur['СвФЛ']['ФИОРус']['@attributes']
            individual_entrepreneur_name = f"{individual_entrepreneur_attributes['Фамилия']} {individual_entrepreneur_attributes['Имя']} {individual_entrepreneur_attributes['Отчество']}"
            return {"inn": inn, "name": individual_entrepreneur_name}
        return {"inn": inn, "name": None}
    except Exception as e:
        print(e)
        return {"inn": inn, "name": None}


@lcthack_router.post(
    "/doc_markup_new",
    response_model_exclude_none=True,
)
@async_timer
async def doc_markup_new(
        file: UploadFile = File(...),
        limit: int = 10,
        session: ClientSession = Depends(get_client_session),
):
    contents = await file.read()

    file_extension = file.filename.split('.')[-1]

    if file_extension == 'csv':
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')), on_bad_lines='skip')
    elif file_extension in ['xls', 'xlsx']:
        df = pd.read_excel(io.BytesIO(contents))
    else:
        return {"error": "Unsupported file type"}

    inns = df['ИНН'].unique().tolist()
    inns = [int(inn) for inn in inns]

    # Get names by INN
    tasks = [get_name_by_inn(inn, session) for inn in inns]
    results = await asyncio.gather(*tasks)

    result_dict = {result["inn"]: result["name"] for result in results}

    names = [name for name in result_dict.values() if name]
    patent_tasks = [fetch_rospatent_data(session, name, limit) for name in names]
    patent_results = await asyncio.gather(*patent_tasks)

    name_to_patents = {name: patents for name, patents in zip(names, patent_results)}

    final_result = []
    for inn, name in result_dict.items():
        patents = name_to_patents.get(name, [])
        if isinstance(patents, dict):
            patents = patents.get(name, [])
        for patent in patents:
            final_result.append({
                "ИНН": inn,
                "name": name,
                "id": patent.id,
                "link": f'https://searchplatform.rospatent.gov.ru/doc/{patent.id}',
                "patent_name": patent.title_ru,
                "patent_name_en": patent.title_en,
                "publication_date": patent.publication_date,
                "application_number": patent.application_number,
                "application_filing_date": patent.application_filing_date,
                "ipc": ' '.join(patent.ipc) if patent.ipc else '',
                "cpc": ' '.join(patent.cpc) if patent.cpc else '',
                # "snippet_ru": patent.snippet_ru,
                "patentees_ru": ' '.join(patent.patentees_ru) if patent.patentees_ru else '',
                "patentees_en": ' '.join(patent.patentees_en) if patent.patentees_en else '',
                "applicants_ru": ' '.join(patent.applicants_ru) if patent.applicants_ru else '',
                "applicants_en": ' '.join(patent.applicants_en) if patent.applicants_en else '',
                "inventors_ru": ' '.join(patent.inventors_ru) if patent.inventors_ru else '',
                "inventors_en": ' '.join(patent.inventors_en) if patent.inventors_en else '',
            })

    results_df = pd.DataFrame(final_result)

    output = io.BytesIO()
    if file_extension == 'csv':
        results_df.to_csv(output, index=False)
        output.seek(0)
        return StreamingResponse(output, headers={"Content-Disposition": "attachment; filename=result.csv"})  # , media_type="text/csv"
    elif file_extension in ['xls', 'xlsx']:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            results_df.to_excel(writer, index=False, sheet_name='Sheet1')
        output.seek(0)
        return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=result.xlsx"})
