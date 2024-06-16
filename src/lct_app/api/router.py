import asyncio
import os
from datetime import datetime
from typing import List, Dict

from aiohttp import ClientSession
from asyncpg import Connection
from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
import aiofiles

from common.db.model import get_inventions_by_inn, get_inventions_by_many_inns, get_industrial_designs_by_many_inns, get_utility_model_by_many_inns
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
    inns = []
    async with aiofiles.open('/opt/app-root/src/files/' + query.file.filename, 'r') as csv_file:
        async for row in aiocsv.AsyncDictReader(csv_file, delimiter=','):
            logging.info(f"{row=}")
            inns.append(row['ИНН'])
            rows.append(row)
    logging.info(f"{inns=}")
    marked_inns_invent = await get_inventions_by_many_inns(db, inns)
    marked_inns_inddes = await get_industrial_designs_by_many_inns(db, inns)
    marked_inns_utimod = await get_utility_model_by_many_inns(db, inns)
    logging.info(f"{marked_inns_invent=}")
    for row in rows:
        flag = False
        if row['ИНН']:
            for marked_row in marked_inns_invent:
                if marked_row['ИНН'] and marked_row['ИНН'] == row['ИНН']:
                    flag = True
                    marked.append(dict(row, **marked_row))
            for marked_row in marked_inns_inddes:
                if marked_row['ИНН'] and marked_row['ИНН'] == row['ИНН']:
                    flag = True
                    marked.append(dict(row, **marked_row))
            for marked_row in marked_inns_utimod:
                if marked_row['ИНН'] and marked_row['ИНН'] == row['ИНН']:
                    flag = True
                    marked.append(dict(row, **marked_row))
        if not flag:
            marked.append(row)
    additional_keys = ["Наименование полное", "ИНН", "registration number", "invention name","utility model name", "mpk", "industrial design name", "publication URL", "mkpo"]
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
    async with aiofiles.open('/opt/app-root/src/lct_app/sometestdata/' + query.file.filename, 'wb') as out_f:
        content = await query.file.read()
        await out_f.write(content)

    return {"some dashboard" : {"data 1": 1, "data 2": 2}}

@lcthack_router.get(
    "/db_dashboard",
    response_model_exclude_none=True,
)
@async_timer
async def db_dashboard(
    session: ClientSession = Depends(get_client_session),
    db: Connection = Depends(get_db_connection),
) :
    return {"some dashboard" : {"data 1": 1, "data 2": 2}}



