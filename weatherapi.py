# -*- coding: utf-8 -*-
import aiohttp, aiofiles, asyncio, json, os, arrow
from itertools import product

api_key = os.environ["API_KEY"]

base_path = "results/"

services = [
    "weather",
    "forecast",
    "air_pollution",
]

headers = {
    "weather": ",".join(
        [
            "time",
            "short",
            "desc",
            "temp",
            "felt_temp",
            "min_temp",
            "max_temp",
            "pressure",
            "humidity",
            "sea_lvl_press",
            "grd_lvl_press",
            "visibility_m",
            "wind_speed",
            "wind_direction",
            "wind_gusts",
            "rain_1h",
            "rain_3h",
            "snow_1h",
            "snow_3h",
            "cloud_coverage",
        ]
    ),
    "forecast": ",".join(
        [
            "time",
            "prediction_time",
            "short",
            "desc",
            "temp",
            "felt_temp",
            "min_temp",
            "max_temp",
            "pressure",
            "humidity",
            "sea_lvl_press",
            "grd_lvl_press",
            "visibility_m",
            "wind_speed",
            "wind_direction",
            "wind_gusts",
            "rain_3h",
            "snow_3h",
            "cloud_coverage",
            "prob_rain",
        ]
    ),
    "air_pollution": ",".join(
        [
            "time",
            "air_quality_index",
            "CO (Carbon monoxide)",
            "NO (Nitrogen monoxide)",
            "NO2 (Nitrogen dioxide)",
            "O3 (Ozone)",
            "SO2 (Sulphur dioxide)",
            "PM2.5 (Fine particles matter)",
            "PM10 (Coarse particulate matter)",
            "NH3 (Ammonia)",
        ]
    ),
}

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


async def write_csv_line(filepath, line_tuple_list, header):
    if not os.path.exists(filepath):
        async with aiofiles.open(filepath, "w") as outfile:
            await outfile.write(
                header
                + "\n"
                + "\n".join(map(lambda x: ",".join(map(str, x)), line_tuple_list))
                + "\n"
            )
    else:
        async with aiofiles.open(filepath, "a") as outfile:
            await outfile.write(
                "\n".join(map(lambda x: ",".join(map(str, x)), line_tuple_list)) + "\n"
            )


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
                    f"{base_path}{curtime.format('YYYYMMDD')}_{resp['location']}_weather.csv",
                    [
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
                            resp["api_answer"]["main"].get("grnd_level", 0),
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
                    ],
                    headers["weather"],
                )
            )
        elif resp["service"] == "air_pollution":
            await asyncio.create_task(
                write_csv_line(
                    f"{base_path}{curtime.format('YYYYMMDD')}_{resp['location']}_air_pollution.csv",
                    [
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
                            .get("so2", -1),
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
                    ],
                    headers["air_pollution"],
                )
            )
        elif resp["service"] == "forecast":
            await asyncio.create_task(
                write_csv_line(
                    f"{base_path}{curtime.format('YYYYMMDD')}_{resp['location']}_forecast.csv",
                    [
                        (
                            curtime.format("YYYY-MM-DD HH:mm"),
                            fcast["dt_txt"],
                            fcast["weather"][0]["main"],
                            fcast["weather"][0]["description"],
                            fcast["main"].get("temp", -100),
                            fcast["main"].get("feels_like", -100),
                            fcast["main"].get("temp_min", -100),
                            fcast["main"].get("temp_max", -100),
                            fcast["main"].get("pressure", 0),
                            fcast["main"].get("humidity", -1),
                            fcast["main"].get("sea_level", 0),
                            fcast["main"].get("grnd_level", 0),
                            fcast.get("visibility", -1),
                            fcast.get("wind", {}).get("speed", 0),
                            fcast.get("wind", {}).get("deg", -1),
                            fcast.get("wind", {}).get("gust", 0),
                            fcast.get("rain", {}).get("3h", 0),
                            fcast.get("snow", {}).get("3h", 0),
                            fcast.get("clouds", {}).get("all", -1),
                            fcast.get("pop", -1),
                        )
                        for fcast in resp["api_answer"]["list"]
                    ],
                    headers["forecast"],
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
