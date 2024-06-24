import time
import aiohttp
import asyncio

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

data = {
    'vyp3CaptchaToken': '',
    'page': '',
    'query': 'УНИЛЕВЕР Н.В.',
    'nameEq': 'on',
    'region': '',
    'PreventChromeAutocomplete': '',
}


async def fetch_data():
    async with aiohttp.ClientSession() as session:
        async with session.post('https://egrul.nalog.ru/', headers=headers, data=data) as response:
            response_json = await response.json()
            print(response_json)

            t = response_json['t']

            current_time_ms = int(time.time() * 1000)
            params = {
                'r': str(current_time_ms),
                '_': str(current_time_ms),
            }

            async with session.get(f'https://egrul.nalog.ru/search-result/{t}', params=params, headers=headers) as response:
                response_json = await response.json()
                await asyncio.sleep(0.21)
                print(response_json)


asyncio.run(fetch_data())
