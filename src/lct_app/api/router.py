import io
import logging

import pandas as pd
from aiohttp import ClientSession
from asyncpg import Connection
from fastapi import APIRouter, Depends, UploadFile, File
from starlette.responses import StreamingResponse

from common.api.dependencies import get_client_session, get_db_connection
from common.db.model import get_inventions_by_inn, get_company_patents_by_inns, get_invention_count, get_industrial_design_count, get_utility_model_count, get_okopf_count, get_marked_patent_count, get_patent_counts_by_inns, get_organisatons_with_patents_count_by_inns, get_okopf_count_by_inns, get_msp_count_by_inns, get_org_count_by_inns, get_msp_classification_category_by_inns, get_msp_classification_type_by_inns, get_org_classification_by_inns
from common.domain.schema import TestRequest
from common.utils.debug import async_timer

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
