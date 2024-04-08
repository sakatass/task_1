import time
import pandas as pd  
from PIL import Image
from io import BytesIO
import numpy as np
import datetime
import asyncio
import aiohttp

sheet_id = '1QX2IhFyYmGDFMvovw2WFz3wAT4piAZ_8hi5Lzp7LjV0'
sheet_name = 'feed'
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

data = pd.read_csv(url)
data.dropna(subset=['image_url'], inplace=True)

imgs_size = {}
start_time = time.time()


async def get_image_size(session, url):
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
    }
    
    async with session.get(url=url, headers=headers) as response:
      
      image_bytes = await response.content.read()
      
      if int(response.status) == 200:
          image = Image.open(BytesIO(image_bytes))

          width, height = image.size
          imgs_size[url] = (f'{width}x{height}')
          
      elif int(response.status) == 404:
          imgs_size[url] = (np.NaN)
          
      else:
          imgs_size[url] = (np.NaN)




async def gather_data(df):

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
        tasks = []
        
        for url in df['image_url'].to_list():
            url = str(url)
            task = asyncio.create_task(get_image_size(session, url))
            tasks.append(task)

        await asyncio.gather(*tasks)


def main():
    asyncio.run(gather_data(data))
    data['SIZE'] = data['image_url'].apply(lambda x: imgs_size[x])
    data.to_csv('data_size.csv', index=False)


if __name__ == "__main__":
    main()
