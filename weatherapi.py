import aiohttp, asyncio, json, os
from itertools import product

api_key = os.environ['API_KEY']

services = [
    'weather',
    'forecast',
    'air_pollution'
    ]

weather_url = "https://api.openweathermap.org/data/2.5/{service}?lat={lat}&lon={lon}&appid={api_key}"

locations_dict = {
    'richtstr_mh': {
        'lat': 51.4261245,
        'lon': 6.8559949
        },
    'elbinger_marl': {
        'lat': 51.6437635,
        'lon': 7.0965704
        }
    }


async def getter_cor(url, **kwargs):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            res = await resp.json()
    kwargs['api_answer'] = res
    return kwargs

async def main():
    res = await asyncio.gather(
            *[
            getter_cor(weather_url.format(**locations_dict[x], api_key = api_key, service = y), location = x, service = y)
            for x, y in product(list(locations_dict.keys()), services)
            ]
        )
    for resp in res:
        with open(f"{resp['location']}_{resp['service']}.json", 'w') as outputfile:
            outputfile.write(json.dumps(resp['api_answer'], indent=2))

if __name__ == '__main__':
    asyncio.run(main())
