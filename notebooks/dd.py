import asyncio
import re
from collections import Counter

import asyncpg
import polars as pl
from asyncpg import Connection
from duckduckgo_search import AsyncDDGS

df = pl.read_csv('/Users/foryourselfand/Documents/patent_hack/data/patent_holders.csv')


async def insert_patent_holder_ddg(connection: Connection, patent_holder: str, inn: int):
    await connection.execute(
        '''
        INSERT INTO patent_holders_duckduckgo (patent_holder, inn) VALUES ($1, $2)
        ON CONFLICT (patent_holder, inn) DO NOTHING;
        ''', patent_holder, inn
    )


async def get_inn_duckduckgo(company_name):
    query = company_name
    query = query.replace('"', '')
    query += ' "ИНН"'
    print(f"{query=}")

    try:
        results = await AsyncDDGS().atext(query, max_results=20)
    except Exception as e:
        print(f"Error: {e}")
        return None

    inn_pattern = re.compile(r"инн\s?(\d+)")
    inn_numbers = []

    for result in results:
        title = result['title'].lower()
        body = result['body'].lower()

        title_inn = inn_pattern.findall(title)
        body_inn = inn_pattern.findall(body)

        inn_numbers.extend(title_inn)
        inn_numbers.extend(body_inn)

    inn_counter = Counter(inn_numbers)
    print(f"{inn_counter=}")

    if not inn_counter:
        return None

    most_common_inn, most_common_count = inn_counter.most_common(1)[0]

    counts = list(inn_counter.values())
    if counts.count(most_common_count) > 1:
        return None

    print(f"{most_common_inn=}")
    return most_common_inn


async def main():
    async with asyncpg.create_pool(
            'postgresql://lcthack_user:lcthack_password@localhost:5400/lcthack'
    ) as pool:
        for i, row in enumerate(df.iter_rows()):
            print(i)
            name = row[0]
            pattern = r'([а-яА-Я]+ [а-яА-Я]+)([А-Я][а-я]+)'

            name = re.sub(pattern, r'\1 \2', name)

            name = name.replace('"', '')

            res = await get_inn_duckduckgo(name)
            if res:
                await insert_patent_holder_ddg(pool, name, res)


if __name__ == '__main__':
    asyncio.run(main())
