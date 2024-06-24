import asyncio
import re
from collections import Counter

import asyncpg
import polars as pl
import undetected_chromedriver as uc
from asyncpg import Connection
from selenium.webdriver.common.by import By
from undetected_chromedriver import ChromeOptions

df = pl.read_csv('/Users/foryourselfand/Documents/patent_hack/data/patent_holders.csv')


async def insert_patent_holder_google(connection: Connection, patent_holder: str, inn: int):
    await connection.execute(
        '''
        INSERT INTO patent_holders_google (patent_holder, inn) VALUES ($1, $2)
        ON CONFLICT (patent_holder, inn) DO NOTHING;
        ''', patent_holder, inn
    )


async def main():
    async with asyncpg.create_pool(
            'postgresql://lcthack_user:lcthack_password@localhost:5400/lcthack'
    ) as pool:
        chrome_options = ChromeOptions()

        prefs = {'profile.default_content_setting_values': {'cookies': 2, 'images': 2, 'javascript': 2,
                                                            'plugins': 2, 'popups': 2, 'geolocation': 2,
                                                            'notifications': 2, 'auto_select_certificate': 2, 'fullscreen': 2,
                                                            'mouselock': 2, 'mixed_script': 2, 'media_stream': 2,
                                                            'media_stream_mic': 2, 'media_stream_camera': 2, 'protocol_handlers': 2,
                                                            'ppapi_broker': 2, 'automatic_downloads': 2, 'midi_sysex': 2,
                                                            'push_messaging': 2, 'ssl_cert_decisions': 2, 'metro_switch_to_desktop': 2,
                                                            'protected_media_identifier': 2, 'app_banner': 2, 'site_engagement': 2,
                                                            'durable_storage': 2, 'stylesheet': 2, 'fonts': 2},
                 }
        # chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        # chrome_options.add_argument('--headless')
        # chrome_options.add_argument('--no-sandbox')
        # chrome_options.add_argument('--disable-dev-shm-usage')
        # chrome_options.add_argument('--disable-gpu')
        # chrome_options.add_argument("--disable-infobars")
        # chrome_options.add_argument("--disable-extensions")

        # chrome_options.add_experimental_option("prefs", prefs)
        driver = uc.Chrome(
            user_data_dir='/Users/foryourselfand/Documents/lct_hack/data/selenium',
            options=chrome_options
        )

        driver.set_page_load_timeout(5)

        print('driver created')

        driver.get('https://google.com')

        for i, row in enumerate(df.iter_rows()):
            print(i)
            while True:
                try:
                    search_input = driver.find_element(By.NAME, 'q')
                    search_input.clear()

                    name = row[0]

                    pattern = r'([а-яА-Я]+ [а-яА-Я]+)([А-Я][а-я]+)'

                    name = re.sub(pattern, r'\1 \2', name)

                    name = name.replace('"', '')
                    name += ' "ИНН"'

                    search_input.send_keys(name)
                    search_input.submit()

                    driver.implicitly_wait(0.5)
                    if i == 0:
                        input()

                    text = driver.find_element(By.TAG_NAME, 'body').text
                    text = re.sub(r'[^a-zа-я0-9]', ' ', text.lower())
                    text = re.sub(r'\s+', ' ', text)

                    inn_pattern = re.compile(r"инн\s?(\d+)")

                    inn_numbers = inn_pattern.findall(text)

                    inn_counter = Counter(inn_numbers)

                    most_common_inn = inn_counter.most_common(1)

                    if most_common_inn:
                        most_common_inn = most_common_inn[0][0]
                        await insert_patent_holder_google(pool, name, most_common_inn)
                    break
                except Exception as e:
                    print(e)
                    input()


if __name__ == '__main__':
    asyncio.run(main())
