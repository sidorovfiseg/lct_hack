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
        # csv header
        # "Наименование"; "ИНН"; "ОГРНИП"; "Дата регистрации"; "ОКВЭД номер"; "ОКВЭД наименование"; "Реестр МСП"; "Вид предпринимательства"; "Категория субъекта"
    )


async def create_table_okopf_map(connection: Connection):
    await connection.execute(
        '''
            CREATE TABLE IF NOT EXISTS okopf_map
            (
                "ОКОПФ (код)" VARCHAR,
                "ОКОПФ (расшифровка)" VARCHAR
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
    await create_table_okopf_map(connection)


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


async def get_msp_organisation_additional_info(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
            select "ИНН", "Реестр МСП", "Вид предпринимательства", "Категория субъекта" from msp_organisation_classification as class
            where class."ИНН" = ANY($1);
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None


async def get_organisation_additional_info(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
            select "inn" as "ИНН", "objecttype" from organisation_classification as class
            where class."inn" = ANY($1);
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None


async def get_invention_count(connection: Connection):
    answer = await connection.fetchval(
        '''
        select count(*) from dataset_invention;
        '''
    )
    return answer


async def get_industrial_design_count(connection: Connection):
    answer = await connection.fetchval(
        '''
        select count(*) from dataset_industrial_design;
        '''
    )
    return answer


async def get_utility_model_count(connection: Connection):
    answer = await connection.fetchval(
        '''
        select count(*) from dataset_utility_model;
        '''
    )
    return answer


async def get_marked_patent_count(connection: Connection, patent_type: str):
    query = '''
    SELECT count
    FROM mv_patent_counts
    WHERE patent_type = $1;
    '''
    answer = await connection.fetchval(query, patent_type)
    return answer


async def get_okopf_count(connection: Connection):
    answer = await connection.fetch(
        '''
            SELECT "ОКОПФ (код)", "ОКОПФ (расшифровка)", count
            FROM mv_okopf_count;
        '''
    )
    return [dict(row) for row in answer] if answer else None


async def get_marked_invention_count_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetchval(
        '''
        select count("registration_number") from companies_patents
        where "company_id" in (select "ID компании" from dataset_organisation 
        where "ИНН" = ANY($1));
        ''', inns
    )
    return answer


async def get_marked_industrial_design_count_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetchval(
        '''
        select count("registration_number") from companies_patents
        where "company_id" in (select "ID компании" from dataset_organisation 
        where "ИНН" = ANY($1));
        ''', inns
    )
    return answer


async def get_marked_utility_model_count_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetchval(
        '''
        select count("registration_number") from companies_patents
        where "company_id" in (select "ID компании" from dataset_organisation 
        where "ИНН" = ANY($1));
        ''', inns
    )
    return answer


async def get_patent_counts_by_inns(connection: Connection, inns: list[str]):
    query = '''
    WITH company_ids AS (
        SELECT "ID компании"
        FROM public.dataset_organisation
        WHERE "ИНН" = ANY($1)
    )
    SELECT 
        COUNT(CASE WHEN cp.patent_type = 'Изобретение' THEN 1 END) AS invention_count,
        COUNT(CASE WHEN cp.patent_type = 'Промышленный образец' THEN 1 END) AS industrial_design_count,
        COUNT(CASE WHEN cp.patent_type = 'Полезная модель' THEN 1 END) AS utility_model_count
    FROM public.companies_patents cp
    WHERE cp.company_id IN (SELECT "ID компании" FROM company_ids);
    '''

    row = await connection.fetchrow(query, inns)
    return row['invention_count'], row['industrial_design_count'], row['utility_model_count']


async def get_organisatons_with_patents_count_by_inns(connection: Connection, inns: list[str]):
    query = '''
    WITH company_ids AS (
        SELECT "ID компании"
        FROM dataset_organisation
        WHERE "ИНН" = ANY($1)
    )
    SELECT COUNT(DISTINCT cp.company_id)
    FROM public.companies_patents cp
    JOIN company_ids ci ON cp.company_id = ci."ID компании";
    '''
    answer = await connection.fetchval(query, inns)
    return answer


async def get_okopf_count_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
            SELECT "ОКОПФ (код)", "ОКОПФ (расшифровка)", COUNT(*)
            FROM dataset_organisation 
            WHERE "ID компании" IN 
            (SELECT "ID компании" FROM dataset_organisation 
            WHERE "ИНН" = ANY($1)) 
            GROUP BY "ОКОПФ (код)", "ОКОПФ (расшифровка)"
            ;
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None


async def get_msp_count_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetchval(
        '''
        select count(*) from msp_organisation_classification
        where "ИНН" = ANY($1);
        ''', inns
    )
    return answer


async def get_msp_classification_type_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
        select "Вид предпринимательства", count(*) from msp_organisation_classification
        where "ИНН" = ANY($1) group by "Вид предпринимательства";
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None


async def get_msp_classification_category_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
        select "Категория субъекта", count(*) from msp_organisation_classification
        where "ИНН" = ANY($1) group by "Категория субъекта";
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None


async def get_org_count_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetchval(
        '''
        select count(*) from organisation_classification
        where "inn" = ANY($1);
        ''', inns
    )
    return answer


async def get_org_classification_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
        select "objecttype", count(*) from organisation_classification
        where "inn" = ANY($1) group by "objecttype";
        ''', inns
    )
    return [dict(row) for row in answer] if answer else None


async def get_company_patents_by_inns(connection: Connection, inn_list):
    inn_tuple = tuple(inn_list)

    query = """
    WITH company_ids AS (
        SELECT 
            org."ID компании", 
            org."Наименование полное", 
            org."Наименование краткое", 
            org."ИНН",
            msp."Реестр МСП",
            msp."Вид предпринимательства",
            msp."Категория субъекта",
            oc."objecttype"
        FROM public.dataset_organisation org
        LEFT JOIN public.msp_organisation_classification msp ON org."ИНН" = msp."ИНН"
        LEFT JOIN public.organisation_classification oc ON org."ИНН" = oc."inn"
        WHERE org."ИНН" = ANY($1)
    )
    SELECT 
        ci."ID компании",
        ci."Наименование полное",
        ci."Наименование краткое",
        ci."ИНН",
        ci."Реестр МСП",
        ci."Вид предпринимательства",
        ci."Категория субъекта",
        ci."objecttype",
        cp.registration_number,
        cp.patent_type,
        COALESCE(di_invention."invention name", di_industrial_design."industrial design name", di_utility_model."utility model name") as patent_name,
        COALESCE(di_invention."mpk", di_industrial_design."mkpo", di_utility_model."mpk") as classification,
        di_industrial_design."publication URL" as industrial_design_url,
        di_utility_model."publication URL" as utility_model_url
    FROM company_ids ci
    JOIN public.companies_patents cp ON ci."ID компании" = cp.company_id
    LEFT JOIN public.dataset_invention di_invention ON cp.registration_number = di_invention."registration number" AND cp.patent_type = 'Изобретение'
    LEFT JOIN public.dataset_industrial_design di_industrial_design ON cp.registration_number = di_industrial_design."registration number" AND cp.patent_type = 'Промышленный образец'
    LEFT JOIN public.dataset_utility_model di_utility_model ON cp.registration_number = di_utility_model."registration number" AND cp.patent_type = 'Полезная модель';
    """

    records = await connection.fetch(query, inn_tuple)

    results = []
    for record in records:
        result = {
            "ID компании": record["ID компании"],
            "Наименование полное": record["Наименование полное"],
            "Наименование краткое": record["Наименование краткое"],
            "ИНН": record["ИНН"],
            "Реестр МСП": record["Реестр МСП"],
            "Вид предпринимательства": record["Вид предпринимательства"],
            "Категория субъекта": record["Категория субъекта"],
            "objecttype": record["objecttype"],
            "registration_number": record["registration_number"],
            "patent_type": record["patent_type"],
            "patent_name": record["patent_name"],
            "classification": record["classification"]
        }
        if record["patent_type"] == 'Промышленный образец':
            result["publication URL"] = record["industrial_design_url"]
        elif record["patent_type"] == 'Полезная модель':
            result["publication URL"] = record["utility_model_url"]
        results.append(result)

    return results


async def insert_patent_holder(connection: Connection, patent_holder: str, inn: int):
    await connection.execute(
        '''
        INSERT INTO patent_holders (patent_holder, inn) VALUES ($1, $2)
        ON CONFLICT (patent_holder, inn) DO NOTHING;
        ''', patent_holder, inn
    )

async def get_full_names_by_inns(connection: Connection, inns: list[str]):
    answer = await connection.fetch(
        '''
            SELECT "Наименование полное", "ИНН"
            FROM dataset_organisation
            WHERE "ИНН" = ANY($1);
        ''', inns
    )
    return {int(row['ИНН']): row['Наименование полное'] for row in answer} if answer else None