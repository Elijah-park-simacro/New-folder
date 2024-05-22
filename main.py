import  asyncio
from config import Settings
import uvicorn
from route import app
from tasks.insert_data import insert_weather_data, insert_commodity_data, insert_plant_data

settings = Settings()
    

async def run_server():
    config = uvicorn.Config(app, host="0.0.0.0", port=8010)
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    
    print('starting...')

    async_tasks = [
        run_server(),
        insert_weather_data(),
        insert_commodity_data(),
        # insert_plant_data()
    ]
    
    await asyncio.gather(*async_tasks)

    print('closing...')

if __name__ == "__main__":
    asyncio.run(main())