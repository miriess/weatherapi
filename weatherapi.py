# -*- coding: utf-8 -*-
import aiohttp, aiofiles, asyncio, json, os, arrow
from itertools import product

api_key = os.environ["API_KEY"]

services = [
    "weather",
    #    'forecast',
    "air_pollution",
]

weather_url = "https://api.openweathermap.org/data/2.5/{service}?lat={lat}&lon={lon}&appid={api_key}&units=metric"

locations_dict = {
    "richtstr_mh": {"lat": 51.4261245, "lon": 6.8559949},
    "elbinger_marl": {"lat": 51.6437635, "lon": 7.0965704},
    "mellenberg_hh": {"lat": 53.6457935, "lon": 10.1613098},
}


async def getter_cor(url, **kwargs):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            res = await resp.json()
    kwargs["api_answer"] = res
    return kwargs


async def write_csv_line(filepath, line_tuple):
    async with aiofiles.open(filepath, "a") as outfile:
        await outfile.write(",".join(map(str, line_tuple)) + "\n")


async def main():
    curtime = arrow.now(tz="Europe/Berlin")
    res = await asyncio.gather(
        *[
            getter_cor(
                weather_url.format(**locations_dict[x], api_key=api_key, service=y),
                location=x,
                service=y,
            )
            for x, y in product(list(locations_dict.keys()), services)
        ]
    )
    for resp in res:
        if resp["service"] == "weather":
            await asyncio.create_task(
                write_csv_line(
                    f"results/{curtime.format('YYYYMMDD')}_{resp['location']}_{resp['service']}.csv",
                    (
                        curtime.format("YYYY-MM-DD HH:mm"),
                        resp["api_answer"]["weather"][0]["main"],
                        resp["api_answer"]["weather"][0]["description"],
                        resp["api_answer"]["main"].get("temp", -100),
                        resp["api_answer"]["main"].get("feels_like", -100),
                        resp["api_answer"]["main"].get("temp_min", -100),
                        resp["api_answer"]["main"].get("temp_max", -100),
                        resp["api_answer"]["main"].get("pressure", 0),
                        resp["api_answer"]["main"].get("humidity", -1),
                        resp["api_answer"]["main"].get("sea_level", 0),
                        resp["api_answer"]["main"].get("ground_level", 0),
                        resp["api_answer"].get("visibility", -1),
                        resp["api_answer"].get("wind", {}).get("speed", 0),
                        resp["api_answer"].get("wind", {}).get("deg", -1),
                        resp["api_answer"].get("wind", {}).get("gust", 0),
                        resp["api_answer"].get("rain", {}).get("1h", 0),
                        resp["api_answer"].get("rain", {}).get("3h", 0),
                        resp["api_answer"].get("snow", {}).get("1h", 0),
                        resp["api_answer"].get("snow", {}).get("3h", 0),
                        resp["api_answer"].get("clouds", {}).get("all", -1),
                    ),
                )
            )
        elif resp["service"] == "air_pollution":
            await asyncio.create_task(
                write_csv_line(
                    f"results/{curtime.format('YYYYMMDD')}_{resp['location']}_{resp['service']}.csv",
                    (
                        curtime.format("YYYY-MM-DD HH:mm"),
                        resp["api_answer"]["list"][0]["main"].get("aqi", -1),
                        resp["api_answer"]["list"][0]
                        .get("components", {})
                        .get("co", -1),
                        resp["api_answer"]["list"][0]
                        .get("components", {})
                        .get("no", -1),
                        resp["api_answer"]["list"][0]
                        .get("components", {})
                        .get("no2", -1),
                        resp["api_answer"]["list"][0]
                        .get("components", {})
                        .get("o3", -1),
                        resp["api_answer"]["list"][0]
                        .get("components", {})
                        .get("s02", -1),
                        resp["api_answer"]["list"][0]
                        .get("components", {})
                        .get("pm2_5", -1),
                        resp["api_answer"]["list"][0]
                        .get("components", {})
                        .get("pm10", -1),
                        resp["api_answer"]["list"][0]
                        .get("components", {})
                        .get("nh3", -1),
                    ),
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
