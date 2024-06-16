from typing import List, Optional, Tuple

from asyncpg import Connection


async def create_table_dataset_organisation(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS dataset_organisation
            (
                    "ID компании" VARCHAR PRIMARY KEY,
                    "Наименование полное" TEXT,
                    "Наименование краткое" TEXT,
                    "ИНН" TEXT,
                    "Юр адрес" TEXT,
                    "Факт адрес" TEXT,
                    "ОГРН" TEXT,
                    "Головная компания (1) или филиал (0)" TEXT,
                    "КПП" TEXT,
                    "ОКОПФ (код)" TEXT,
                    "ОКОПФ (расшифровка)" TEXT,
                    "ОКВЭД2" TEXT,
                    "ОКВЭД2 расшифровка" TEXT,
                    "Дата создания" DATE,
                    "статус по ЕГРЮЛ " TEXT,
                    "ОКФС код" TEXT,
                    "ОКФС (форма собственности)" TEXT,
                    "Компания действующая (1) или нет (0)" TEXT,
                    "id Компании-наследника (реорганизация и др)" TEXT, 
                    "телефоны СПАРК" TEXT,
                    "ФИО директора" TEXT,
                    "Название должности" TEXT,
                    "доп. ОКВЭД2" TEXT
            );
    '''
    )

async def create_table_dataset_invention(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS dataset_invention
            (
                    "registration number" VARCHAR PRIMARY KEY,
                    "registration date" DATE,
                    "application number" TEXT,
                    "application date" DATE,
                    "authors" TEXT,
                    "authors in latin" TEXT,
                    "patent holders" TEXT,
                    "patent holders in latin" TEXT,
                    "correspondence address" TEXT,
                    "correspondence address in latin" TEXT,
                    "invention name" TEXT,
                    "patent starting date" TEXT, 
                    "Crimean invention application number for state registration in Ukraine" TEXT,
                    "Crimean invention application date for state registration in Ukraine" TEXT,
                    "Crimean invention patent number in Ukraine" TEXT,
                    "receipt date of additional data to application" TEXT,
                    "date of application to which additional data has been received" TEXT,
                    "number of application to which additional data has been received" TEXT,
                    "initial application number" TEXT,
                    "initial application date" TEXT,
                    "initial application priority date" TEXT,
                    "previous application number" TEXT,
                    "previous application date" TEXT,
                    "paris convention priority number" TEXT,
                    "paris convention priority date" TEXT,
                    "paris convention priority country code" TEXT,
                    "PCT application examination start date" TEXT,
                    "PCT application number" TEXT,
                    "PCT application date" TEXT,
                    "PCT application publish number" TEXT,
                    "PCT application publish date" TEXT,
                    "EA application number" TEXT,
                    "EA application date" TEXT,
                    "EA application publish number" TEXT,
                    "EA application publish date" TEXT,
                    "application publish date" TEXT,
                    "application publish number" TEXT,
                    "patent grant publish date" TEXT,
                    "patent grant publish number" TEXT,
                    "revoked patent number" TEXT,
                    "information about the obligation to conclude contract of alienation" TEXT,
                    "expiration date" TEXT,
                    "invention formula numbers for which patent term is prolonged" TEXT,
                    "additional patent" TEXT,
                    "actual" TEXT,
                    "mpk" TEXT
            );
    '''
    )

async def create_table_dataset_industrial_design(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS dataset_industrial_design
            (
                "registration number" VARCHAR PRIMARY KEY,
                "registration date" DATE,
                "application number" TEXT,
                "application date" DATE,
                "authors" TEXT,
                "authors in latin" TEXT,
                "patent holders" TEXT,
                "patent holders in latin" TEXT,
                "correspondence address" TEXT,
                "correspondence address in latin" TEXT,
                "industrial design name" TEXT,
                "patent starting date" TEXT,
                "Crimean industrial design application number for state registration in Ukraine" TEXT,
                "Crimean industrial design application date for state registration in Ukraine" TEXT,
                "Crimean industrial design patent number in Ukraine" TEXT,
                "receipt date of additional data to application" TEXT,
                "date of application to which additional data has been received" TEXT,
                "number of application to which additional data has been received" TEXT,
                "initial application number" TEXT,
                "initial application date" TEXT,
                "initial application priority date" TEXT,
                "previous application number" TEXT,
                "previous application date" TEXT,
                "paris convention priority number" TEXT,
                "paris convention priority date" TEXT,
                "paris convention priority country code" TEXT,
                "patent grant publish date" TEXT,
                "patent grant publish number" TEXT,
                "revoked patent number" TEXT,
                "expiration date" TEXT,
                "numbers of list of essential features for which patent term is prolonged" TEXT,
                "industrial designs names and number for which patent term is prolonged" TEXT,
                "actual" TEXT,
                "publication URL" TEXT,
                "actual_date" TEXT,
                "mkpo" TEXT
        );
    '''
    )

async def create_table_dataset_utility_model(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS dataset_utility_model
            (
                "registration number" VARCHAR PRIMARY KEY,
                "registration date" DATE,
                "application number" TEXT,
                "application date" DATE,
                "authors" TEXT,
                "authors in latin" TEXT,
                "patent holders" TEXT,
                "patent holders in latin" TEXT,
                "correspondence address" TEXT,
                "correspondence address in latin" TEXT,
                "utility model name" TEXT,
                "patent starting date" TEXT,
                "Crimean utility model application number for state registration in Ukraine" TEXT,
                "Crimean utility model application date for state registration in Ukraine" TEXT,
                "Crimean utility model patent number in Ukraine" TEXT,
                "receipt date of additional data to application" TEXT,
                "date of application to which additional data has been received" TEXT,
                "number of application to which additional data has been received" TEXT,
                "initial application number" TEXT,
                "initial application date" TEXT,
                "initial application priority date" TEXT,
                "previous application number" TEXT,
                "previous application date" TEXT,
                "paris convention priority number" TEXT,
                "paris convention priority date" TEXT,
                "paris convention priority country code" TEXT,
                "PCT application examination start date" TEXT,
                "PCT application number" TEXT,
                "PCT application date" TEXT,
                "PCT application publish number" TEXT,
                "PCT application publish date" TEXT,
                "patent grant publish date" TEXT,
                "patent grant publish number" TEXT,
                "revoked patent number" TEXT,
                "expiration date" TEXT,
                "utility model formula numbers for which patent term is prolonged" TEXT,
                "actual" TEXT,
                "publication URL" TEXT,
                "mpk" TEXT
        );
    '''
    )

async def create_table_id_to_regnum_invent(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS id_to_regnum_invent
            (
                "ID компании" VARCHAR,
                "registration number" VARCHAR
            );
    '''
    )

async def create_table_id_to_regnum_inddes(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS id_to_regnum_inddes
            (
                "ID компании" VARCHAR,
                "registration number" VARCHAR
            );
    '''
    )

async def create_table_id_to_regnum_utimod(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS id_to_regnum_utimod
            (
                "ID компании" VARCHAR,
                "registration number" VARCHAR
            );
    '''
    )

async def create_table_organisation_classifiaction(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS organisation_classification
            (
                ObjectType VARCHAR,
                CompanyID VARCHAR,
                INN VARCHAR,
                Name TEXT
            );
    '''
    )

async def create_table_msp_organisation_classifiaction(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS msp_organisation_classification
            (
                "Наименование" TEXT,
                "ИНН" VARCHAR,
                "ОГРНИП" VARCHAR,
                "Дата регистрации" TEXT,
                "ОКВЭД номер" VARCHAR,
                "ОКВЭД наименование" VARCHAR,
                "Реестр МСП" VARCHAR,
                "Вид предпринимательства" TEXT,
                "Категория субъекта" TEXT
            );
    '''
    )

async def create_tables(connection: Connection):
    await create_table_dataset_organisation(connection)
    await create_table_dataset_invention(connection)
    await create_table_dataset_industrial_design(connection)
    await create_table_dataset_utility_model(connection)
    await create_table_id_to_regnum_invent(connection)
    await create_table_id_to_regnum_inddes(connection)
    await create_table_id_to_regnum_utimod(connection)
    await create_table_organisation_classifiaction(connection)
    await create_table_msp_organisation_classifiaction(connection)

async def get_inventions_by_inn(connection: Connection, inn):
    answer = await connection.fetch(
        f'''
            select org."Наименование полное", org."ИНН", di."registration number", di."invention name" from dataset_organisation as org
            join id_to_regnum_invent as itr
            on org."ID компании" = itr."ID компании"
            join dataset_invention as di
            on itr."registration number" = di."registration number"
            where org."ИНН" = '{inn}';
        '''
    )
    return [dict(row) for row in answer] if answer else None

async def get_inventions_by_many_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
            select org."Наименование полное", org."ИНН", di."registration number", di."invention name", di."mpk" from dataset_organisation as org
            join id_to_regnum_invent as itr
            on org."ID компании" = itr."ID компании"
            join dataset_invention as di
            on itr."registration number" = di."registration number"
            where org."ИНН" = ANY($1);
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None

async def get_industrial_designs_by_many_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
            select org."Наименование полное", org."ИНН", di."registration number", di."industrial design name", di."publication URL", di."mkpo" from dataset_organisation as org
            join id_to_regnum_inddes as itr
            on org."ID компании" = itr."ID компании"
            join dataset_industrial_design as di
            on itr."registration number" = di."registration number"
            where org."ИНН" = ANY($1);
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None

async def get_utility_model_by_many_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
            select org."Наименование полное", org."ИНН", di."registration number", di."utility model name", di."publication URL", di."mpk" from dataset_organisation as org
            join id_to_regnum_utimod as itr
            on org."ID компании" = itr."ID компании"
            join dataset_utility_model as di
            on itr."registration number" = di."registration number"
            where org."ИНН" = ANY($1);
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None

async def get_organisation_additional_info(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
            select org."Наименование полное", org."ИНН", di."registration number", di."invention name" from dataset_organisation as org
            join id_to_regnum_invent as itr
            on org."ID компании" = itr."ID компании"
            join dataset_invention as di
            on itr."registration number" = di."registration number"
            where org."ИНН" = ANY($1);
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None