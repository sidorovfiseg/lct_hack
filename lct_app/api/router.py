import asyncio
import os
from datetime import datetime
from typing import List, Dict

from aiohttp import ClientSession
from asyncpg import Connection
from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
import aiofiles

from common.db.model import get_inventions_by_inn, get_inventions_by_many_inns, get_industrial_designs_by_many_inns, get_utility_model_by_many_inns, get_okopf_count, get_msp_organisation_additional_info, get_organisation_additional_info
from common.db.model import get_invention_count, get_utility_model_count, get_marked_invention_count, get_industrial_design_count, get_marked_utility_model_count, get_marked_industrial_design_count, get_organisatons_with_patents_count, get_msp_count_by_inns, get_org_count_by_inns, get_msp_classification_category_by_inns, get_msp_classification_type_by_inns, get_org_classification_by_inns
from common.db.model import get_marked_invention_count_by_inns, get_marked_utility_model_count_by_inns, get_marked_industrial_design_count_by_inns, get_organisatons_with_patents_count_by_inns, get_okopf_count_by_inns
from common.api.dependencies import get_client_session, get_db_connection
from common.domain.schema import MarkupRequest, TestRequest
from common.utils.debug import async_timer

import aiocsv

import logging

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
) :
    test_res = await get_inventions_by_inn(db, '9729275828')
    logging.info(f"{test_res=}")

    return test_res

@lcthack_router.post(
    "/doc_markup",
    response_model_exclude_none=True,
)
@async_timer
async def markup(
    query: MarkupRequest = Depends(),
    session: ClientSession = Depends(get_client_session),
    db: Connection = Depends(get_db_connection),
) :
    async with aiofiles.open('/opt/app-root/src/files/' + query.file.filename, 'wb') as out_f:
        content = await query.file.read()
        await out_f.write(content)
    rows = []
    marked = []
    inns = set()
    async with aiofiles.open('/opt/app-root/src/files/' + query.file.filename, 'r') as csv_file:
        async for row in aiocsv.AsyncDictReader(csv_file, delimiter=','):
            inns.add(row['ИНН'])
            rows.append(row)
    marked_inns_invent = await get_inventions_by_many_inns(db, inns)
    marked_inns_inddes = await get_industrial_designs_by_many_inns(db, inns)
    marked_inns_utimod = await get_utility_model_by_many_inns(db, inns)
    msp_add_info = await get_msp_organisation_additional_info(db, inns)
    add_info = await get_organisation_additional_info(db, inns)
    for row in rows:
        flag = False
        if row['ИНН']:
            if marked_inns_invent:
                for marked_row in marked_inns_invent:
                    if marked_row['ИНН'] and marked_row['ИНН'] == row['ИНН']:
                        flag = True
                        marked.append(dict(row, **marked_row))
            if marked_inns_inddes:
                for marked_row in marked_inns_inddes:
                    if marked_row['ИНН'] and marked_row['ИНН'] == row['ИНН']:
                        flag = True
                        marked.append(dict(row, **marked_row))
            if marked_inns_utimod:
                for marked_row in marked_inns_utimod:
                    if marked_row['ИНН'] and marked_row['ИНН'] == row['ИНН']:
                        flag = True
                        marked.append(dict(row, **marked_row))
            if msp_add_info:
                for marked_row in msp_add_info:
                    if marked_row['ИНН'] and marked_row['ИНН'] == row['ИНН']:
                        flag = True
                        marked.append(dict(row, **marked_row))
            if add_info:
                for marked_row in add_info:
                    if marked_row['ИНН'] and marked_row['ИНН'] == row['ИНН']:
                        flag = True
                        marked.append(dict(row, **marked_row))
        if not flag:
            marked.append(row)
    additional_keys = ["Наименование полное", "ИНН","objecttype","Вид предпринимательства", "registration number", "invention name","utility model name", "mpk", "industrial design name", "publication URL", "mkpo","Реестр МСП",  "Категория субъекта" ]
    all_keys = list(rows[0].keys()) + additional_keys
    uniq_keys = set(all_keys)
    keys = sorted(uniq_keys, key=all_keys.index)
    async with aiofiles.open('/opt/app-root/src/files/'+ 'marked_' + query.file.filename, mode = 'w', encoding='utf-8') as csv_file:
        writer = aiocsv.AsyncDictWriter(csv_file, keys, restval='')
        await writer.writeheader()
        for row in marked:
            await writer.writerow(row)
    return FileResponse('/opt/app-root/src/files/'+ 'marked_' + query.file.filename)

@lcthack_router.post(
    "/doc_dashboard",
    response_model_exclude_none=True,
)
@async_timer
async def doc_dashboard(
    query: MarkupRequest = Depends(),
    session: ClientSession = Depends(get_client_session),
    db: Connection = Depends(get_db_connection),
) :
    async with aiofiles.open('/opt/app-root/src/files/' + query.file.filename, 'wb') as out_f:
        content = await query.file.read()
        await out_f.write(content)
    inns = set()
    async with aiofiles.open('/opt/app-root/src/files/' + query.file.filename, 'r') as csv_file:
        async for row in aiocsv.AsyncDictReader(csv_file, delimiter=','):
            inns.add(row['ИНН'])
    marked_inv_count_by_inns = await get_marked_invention_count_by_inns(db, inns)
    marked_ind_count_by_inns = await get_marked_industrial_design_count_by_inns(db, inns)
    marked_uti_count_by_inns = await get_marked_utility_model_count_by_inns(db, inns)
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
            "МСП":{
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
) :

    inv_count = await get_invention_count(db)
    ind_count = await get_industrial_design_count(db)
    uti_count = await get_utility_model_count(db)
    marked_inv_count = await get_marked_invention_count(db)
    marked_ind_count = await get_marked_industrial_design_count(db)
    marked_uti_count = await get_marked_utility_model_count(db)
    org_with_pat = await get_organisatons_with_patents_count(db)
    okopf = await get_okopf_count(db)
    return {"Количество изобретений": inv_count, 
            "Количество промышленных образцов": ind_count,
            "Количество полезных моделей": uti_count, 
            "Количество размеченных изобретений": marked_inv_count,
            "Количество размеченных промышленных образцов": marked_ind_count,
            "Количество размеченных полезных моделей": marked_uti_count,
            "Количество размеченных организаций": org_with_pat,
            "Разметка по ОКОПФ": okopf}