import aiohttp
from datetime import datetime, timezone
import motor.motor_asyncio
from config import Settings
import asyncio
import collections
from motor.motor_asyncio import AsyncIOMotorClient
import re

settings = Settings()

async def insert_weather_data():
    async def fetch_weather_data(client, city_name, lat, lon):
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid=3f38cfdbb83e2d20e9236ea18db3dd5d&units=Imperial"
       
        utc_datetime = datetime.now(tz=timezone.utc)
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    tag_list = []

                    for key, value in data['current'].items():
                        if key != 'dt':
                            feature = f"{city_name}, {key}"
                            document = {
                                "feature": feature,
                                "name": key,
                                "value": value,
                                'timestamp': utc_datetime
                            }
                            tag_list.append(document)

                    if tag_list:
                        await client['weather']['weather_data'].insert_many(tag_list)

    while True:
        client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongoURL)
        cities = [
            {"name": "seosan", "lat": 36.7844993, "lon": 126.4503169},
            {"name": "kerteh", "lat": 4.586588, "lon": 103.431057},
            {"name": "fort dodge", "lat": 42.5105529, "lon": -94.3138613},
        ]

        tasks = [fetch_weather_data(client, city["name"], city["lat"], city["lon"]) for city in cities]
        await asyncio.gather(*tasks)

        await asyncio.sleep(3600)

async def insert_commodity_data():
    async def process_url(url):
        client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongoURL)
        utc_datetime = datetime.now(tz=timezone.utc)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    tag_list = []

                    for item in data:
                        try:
                            feature = f"{item['category']}, {item['name']}"
                            document = {
                                "feature": feature,
                                "name": item.get('name'),
                                "value": item.get('price'),
                                'timestamp': utc_datetime,
                            }
                            tag_list.append(document)
                        except TypeError:
                            print(f"TypeError: item is not a dictionary. Skipped item: {item}")
                            continue
                        except KeyError:
                            print(f"KeyError: Missing 'category' or 'name' in item. Skipped item: {item}")
                            continue

                    if isinstance(tag_list, list) and len(tag_list) > 0:
                        await client['commodity']['commodity_data'].insert_many(tag_list)
                    else:
                        print("tag_list is not a valid list or is empty")

        
        
    url_list = ['https://api.commoditic.com/api/v1/commodities?key=6a40053bedaca5186ad066c9dba93d1a411f4bde&category=metals',
                'https://api.commoditic.com/api/v1/commodities?key=6a40053bedaca5186ad066c9dba93d1a411f4bde&category=energy',
                'https://api.commoditic.com/api/v1/commodities?key=6a40053bedaca5186ad066c9dba93d1a411f4bde&category=agricultural',
                'https://api.commoditic.com/api/v1/commodities?key=6a40053bedaca5186ad066c9dba93d1a411f4bde&category=livestock'
                ]
    while True:
        for url in url_list:
            await process_url(url)
        await asyncio.sleep(3600)


# hdo_client = AsyncIOMotorClient("mongodb://localhost:27018")
hdo_client = AsyncIOMotorClient("mongodb+srv://simacro:%21tr%40WiZ%40cQX_65%23@serverlessinstance0.mnif2pk.mongodb.net/")

data_server_client = AsyncIOMotorClient(settings.mongoURL)

hdo_plant_collection = hdo_client.hdo_fcc['plant_data']
data_server_plant_db = data_server_client.hdo_fcc
collection_name = 'plant_data'
batch_count = 10000


def transform_feature(feature):
    # "Unit1, pirtdb\E19FC305"
    match = re.match(r"(.*),\s*pirtdb\\(.*)", feature)
    if match:
        return f"FCC, {match.group(2)}"
    return feature

async def plant_data_loop():

    total_documents = await hdo_plant_collection.count_documents({})
    processed_count = 0
    batches = collections.defaultdict(list)
    delete_ids = collections.defaultdict(list)

    async for doc in hdo_plant_collection.find():
        original_id = doc['_id']
        doc.pop('_id', None)
        timestamp = doc['timestamp']
        doc['feature'] = transform_feature(doc['feature'])

        year_month = timestamp.strftime('%Y-%m')
        new_collection = data_server_plant_db[f"{hdo_plant_collection.name}_{year_month}"]

        existing_doc = await new_collection.find_one({
            'timestamp': doc['timestamp'],
            'feature': doc['feature']
        })
        delete_ids[year_month].append(original_id)
        batches[year_month].append(doc)
            
        # if len(batches[year_month]) >= batch_count:
        #     await insert_batches(batches, delete_ids)

        if existing_doc is None:
            batches[year_month].append(doc)
            
            if len(batches[year_month]) >= batch_count:
                await insert_batches(batches, delete_ids)



    await insert_batches(batches, delete_ids)


async def insert_batches(batches, delete_ids ):
        for year_month, batch in list(batches.items()):
            if len(batch) > 0:
                new_collection_name = f"{collection_name}_{year_month}"
                new_collection = data_server_plant_db[new_collection_name]
                existing_collections = await data_server_plant_db.list_collection_names()
                if new_collection_name not in existing_collections:
                    await data_server_plant_db.create_collection(new_collection_name, timeseries={
                        'timeField': 'timestamp',
                        'metaField': 'metadata',
                        'granularity': 'seconds'
                    })
                    await new_collection.create_index([('timestamp', 1)])

                insert_result = await data_server_plant_db[new_collection_name].insert_many(batch)
                # print(f"Inserted {len(insert_result.inserted_ids)} documents from source collection for {year_month}")

                batches[year_month] = []

        for year_month in delete_ids:
            if delete_ids[year_month]:
                delete_result = await hdo_plant_collection.delete_many({'_id': {'$in': delete_ids[year_month]}})
                # print(f"Deleted {delete_result.deleted_count} documents from source collection for {year_month} (cleanup)")
                delete_ids[year_month] = []


async def insert_plant_data():
    while True:
        await plant_data_loop()
        await asyncio.sleep(30)