import io
import logging
import uuid

import pandas as pd
from aiohttp import ClientSession
from asyncpg import Connection
from fastapi import APIRouter, Depends, UploadFile, File
from langchain_community.chat_models.gigachat import GigaChat
from langchain_core.messages import HumanMessage, SystemMessage
from sklearn.cluster import DBSCAN
from starlette.responses import StreamingResponse

from common.api.dependencies import get_client_session, get_db_connection
from common.db.model import get_inventions_by_inn, get_company_patents_by_inns, get_invention_count, get_industrial_design_count, get_utility_model_count, get_okopf_count, get_marked_patent_count, get_patent_counts_by_inns, get_organisatons_with_patents_count_by_inns, get_okopf_count_by_inns, get_msp_count_by_inns, get_org_count_by_inns, get_msp_classification_category_by_inns, get_msp_classification_type_by_inns, get_org_classification_by_inns
from common.domain.schema import TestRequest
from common.utils.debug import async_timer
from lct_app.embeddings.main import GigaChatEmbeddingFunction
from lct_app.embeddings.setting import giga_chat_api_config

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
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
    inns = df['ИНН'].tolist()
    inns = [int(inn) for inn in inns]
    results = await get_company_patents_by_inns(db, inns)

    results_df = pd.DataFrame(results)
    merged_df = pd.merge(df, results_df, on='ИНН', how='left')

    output = io.StringIO()
    merged_df.to_csv(output, index=False)
    output.seek(0)

    return StreamingResponse(output, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=result.csv"})


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
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
    inns = df['ИНН'].tolist()

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
        "Организации":
            {
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
