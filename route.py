from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
import motor.motor_asyncio
from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import status, Request, HTTPException
import pytz
from config import Settings
import asyncio

settings = Settings()


app = FastAPI( title="Simacro", summary="Process Canvas API Document", version="2")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.get("/")
async def read_root():
    return "Data Server working"


class TagInfo(BaseModel):
    category: str = Field(title='data source name')
    tag: List[str] = Field(title='data target tag')
    time_range: Optional[List[int]] = Field(default=None, title='tag last update')
    latest_count: Optional[int] = Field(default=None, title='Number of latest records to fetch')

async def process_tag_info(tag_info: TagInfo):
    if tag_info.time_range is None and tag_info.latest_count is None:
        raise HTTPException(status_code=400, detail="Either time_range or latest_count must be provided.")
    if tag_info.time_range is not None and tag_info.latest_count is not None:
        raise HTTPException(status_code=400, detail="Either provide time_range or latest_count, not both.")

    feature = ", ".join(tag_info.tag)
    category = tag_info.category
    client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongoURL)
    if category in ['model', 'plant']:
        db = client['hdo_fcc']

        if tag_info.time_range:
            start_timestamp, end_timestamp = tag_info.time_range
            start_date = datetime.fromtimestamp(start_timestamp,tz= pytz.timezone('UTC'))
            end_date = datetime.fromtimestamp(end_timestamp,tz= pytz.timezone('UTC'))

            start_month = datetime(start_date.year, start_date.month, 1)
            end_month = datetime(end_date.year, end_date.month, 1)
 

            current_month = start_month
            collections_to_query = []
            while current_month <= end_month:
                month_collection = f"{category}_data_{current_month.strftime('%Y-%m')}"
                collections_to_query.append(month_collection)
                if current_month.month == 12:
                    current_month = current_month.replace(year=current_month.year + 1, month=1)
                else:
                    current_month = current_month.replace(month=current_month.month + 1)

            tasks = []
            for month_collection in collections_to_query:
                collection = db[month_collection]
                task = collection.find({"timestamp": {"$gte": start_date, "$lt": end_date}, "feature": feature}).to_list(None)
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            result = []
            for r in results:
                for doc in r:
                    result.append([doc["timestamp"].replace(tzinfo=timezone.utc).timestamp(), doc["value"]])
            
            result.sort(key=lambda x: x[0])

        elif tag_info.latest_count:
            current_month = datetime.now()

            result = []
            total_fetched = 0

            while total_fetched < tag_info.latest_count:
                month_collection = f"{category}_data_{current_month.strftime('%Y-%m')}"
                collection = db[month_collection]
                cursor = collection.find({"feature": feature}).sort("timestamp", -1).limit(tag_info.latest_count - total_fetched)
                monthly_results = await cursor.to_list(None)
                
                result.extend([[doc["timestamp"].timestamp(), doc["value"]] for doc in monthly_results])
                total_fetched += len(monthly_results)

                if current_month.month == 1:
                    current_month = current_month.replace(year=current_month.year - 1, month=12)
                else:
                    current_month = current_month.replace(month=current_month.month - 1)
                
                if current_month.year < 2000:  
                    break

        
    elif category in ['commodity', 'weather']:
        db = client[tag_info.category]
        collection = db[f'{tag_info.category}_data']
        feature = ", ".join(tag_info.tag)

        result = []

        if tag_info.time_range:
            start_timestamp, end_timestamp = tag_info.time_range
            start_date = datetime.fromtimestamp(start_timestamp,tz= pytz.timezone('UTC'))
            end_date = datetime.fromtimestamp(end_timestamp,tz= pytz.timezone('UTC'))
            matched_documents = collection.find({"timestamp": {"$gte": start_date, "$lt": end_date}, "feature": feature})
            result = [[doc["timestamp"].replace(tzinfo=timezone.utc).timestamp(), doc["value"]] async for doc in matched_documents]
        elif tag_info.latest_count:
            cursor = collection.find({"feature": feature}).sort("timestamp", -1).limit(tag_info.latest_count)
            result = [[doc["timestamp"].replace(tzinfo=timezone.utc).timestamp(), doc["value"]] async for doc in cursor]
    
    client.close()

    return {
        "category": category,
        "tag": tag_info.tag,
        "data": result
    }

@app.post('/get_tag_data', status_code=status.HTTP_200_OK)
async def get_tag_data(tag_info: TagInfo):
    result = await process_tag_info(tag_info)
    return result['data']


class TagInfoList(BaseModel):
    tag_infos: List[TagInfo]

@app.post('/get_tag_data_list', status_code=status.HTTP_200_OK)
async def get_tag_data_list(tag_info_list: TagInfoList):
    results = await asyncio.gather(*[process_tag_info(tag_info) for tag_info in tag_info_list.tag_infos])
    return results



class StreamInfo(BaseModel):
    stream: List[str] = Field(title='data target tag')

@app.post('/stream_info', status_code=status.HTTP_200_OK)
async def get_stream_data(stream_info: StreamInfo): 
    client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongoURL)
    db = client['hdo_fcc']
    regex_pattern = ', '.join(stream_info.stream) + '(,|$)'

    matched_documents = db['model_list'].find({"feature": {"$regex": regex_pattern}})
    stream_array = [doc async for doc in matched_documents]

    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    results = []
    
    for offset in range(0, 12):  
        month_offset = (current_month - offset - 1) % 12 + 1
        year_offset = current_year - (current_month - offset - 1) // 12
        collection_name = f"model_data_{year_offset}-{month_offset:02d}"
        collection = db[collection_name]
        
        for item in stream_array:
            latest_data = await collection.find_one({"feature": item['feature']}, sort=[("timestamp", -1)])
            if latest_data:
                results.append(latest_data)
                stream_array.remove(item)
        
        if not stream_array or offset == 11:
            break


    def find_or_create_node(current_node, part):
        for node in current_node:
            if node['name'] == part:
                return node
        new_node = {'name': part}
        current_node.append(new_node)
        return new_node

    model_tree = []
    for item in results:
        path = item['feature'].split(', ')
        current_node = model_tree
        for part in path[:-1]:
            current_node = find_or_create_node(current_node, part)
            if 'subnode' not in current_node:
                current_node['subnode'] = []
            current_node = current_node['subnode']
        last_part = path[-1]
        last_node = find_or_create_node(current_node, last_part)
        for key, value in item.items():
            if key not in ['feature', 'name', 'subnode', '_id', 'timestamp']:
                last_node[key] = value

    result = model_tree[0]['subnode'][0]['subnode']

    client.close()
    return result


@app.post('/get_tag_data', status_code=status.HTTP_200_OK)
async def getTag(tag_info: TagInfo): 
    if tag_info.time_range is None and tag_info.latest_count is None:
        raise HTTPException(status_code=400, detail="Either time_range or latest_count must be provided.")
    if tag_info.time_range is not None and tag_info.latest_count is not None:
        raise HTTPException(status_code=400, detail="Either provide time_range or latest_count, not both.")
    
    client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongoURL)
    db = client[tag_info.category]
    collection = db[f'{tag_info.category}_data']
    feature = ", ".join(tag_info.tag)

    result = []

    if tag_info.time_range:
        start_timestamp, end_timestamp = tag_info.time_range
        start_date = datetime.fromtimestamp(start_timestamp,tz= pytz.timezone('UTC'))
        end_date = datetime.fromtimestamp(end_timestamp,tz= pytz.timezone('UTC'))
        matched_documents = collection.find({"timestamp": {"$gte": start_date, "$lt": end_date}, "feature": feature})
        result = [[doc["timestamp"].replace(tzinfo=timezone.utc).timestamp(), doc["value"]] async for doc in matched_documents]
    elif tag_info.latest_count:
        cursor = collection.find({"feature": feature}).sort("timestamp", -1).limit(tag_info.latest_count)
        result = [[doc["timestamp"].replace(tzinfo=timezone.utc).timestamp(), doc["value"]] async for doc in cursor]

    client.close()
    return result


class HierarchyInfo(BaseModel):
    category: str = Field(title='data source name')

@app.post("/get_hierarchy", status_code=status.HTTP_200_OK)
async def get_hierarchy(hierarchy_info: HierarchyInfo):
    category = hierarchy_info.category
    result = []

    client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongoURL)
    if category == 'model' or category == 'plant':
        col = client['hdo_fcc'][f"{category}_list"]
    else:
        col = client[category][f"{category}_list"]
    cursor = col.find()
    tag_list = [doc async for doc in cursor]

    def find_or_create_node(current_node, part):
        for node in current_node:
            if node['name'] == part:
                return node
        new_node = {'name': part}
        current_node.append(new_node)
        return new_node

    for item in tag_list:
        path = item['feature'].split(', ')
        current_node = result
        for part in path[:-1]:
            current_node = find_or_create_node(current_node, part)
            if 'subnode' not in current_node:
                current_node['subnode'] = []
            current_node = current_node['subnode']
        last_part = path[-1]
        last_node = find_or_create_node(current_node, last_part)
        for key, value in item.items():
            if key not in ['feature', 'name', 'subnode', '_id']:
                last_node[key] = value
    
    client.close()
    return result