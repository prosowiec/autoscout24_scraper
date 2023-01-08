from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.request import urlopen
import ssl 
import certifi
import regex as re
import pandas as pd
import os
import datetime
from multiprocessing.pool import ThreadPool
import concurrent.futures
from tqdm import tqdm

def format_price(price):
  space = 0
  for i in range(0,len(price)):
    if price[i] == ' ':
      space = i
      break
  price = price[space:]
  price = int(price.replace(',',''))
  return price

def format_power(power):
  power_pattern = re.compile('(?<=\()(.*?)(?=\))')
  power = re.findall(power_pattern,power)
  power = str(power[0])
  space =0
  for i in range(0,len(power)):
    if power[i] == ' ':
      space = i
      break
  power_unit = power[space+1:]
  power = power[:space]

  return power,power_unit

def format_mileage(mileage):
  for i in range(0,len(mileage)):
    if mileage[i] == ' ':
      space = i
      break

  mileage_unit = mileage[space+1:]
  mileage = mileage[:space]
  mileage = int(mileage.replace(',',''))
  return mileage,mileage_unit

def format_year(year):
  space = year.index(r'/')
  year = year[space+1:]
  return year

def find_brand(url):
  brand_pattern = re.compile('(?<=https:\/\/www\.autoscout24\.com\/lst\/)(.*?)(?=\/)')
  brand = re.search(brand_pattern,url)
  brand = str(brand.group(0))
  return brand
  
def find_model(url,brand):
  model_pattern = re.compile(f'(?<=https:\/\/www\.autoscout24\.com\/lst\/{brand}\/)(.*?)(?=\?)')
  model = re.search(model_pattern,url)
  model = str(model.group(0))
  return model

def numer_of_pages(url):
  r = urlopen(url, context=ssl.create_default_context(cafile=certifi.where()))
  soup = BeautifulSoup(r, "html.parser")
  content = soup.find_all("li", {"class": "pagination-item--disabled pagination-item--page-indicator"})
  try:
    content = str(content[0])
    pages_pattern = re.compile('(?<= / <!-- -->)(.*?)(?=<\/span><\/li>)')
    pages = re.search(pages_pattern,content)
    pages = int(pages.group(0))
    return pages
  except:
    return -1 


def scrape_autoscout(pre_url):
  brand = find_brand(pre_url)
  model = find_model(pre_url,brand)
  pages = numer_of_pages(pre_url)
  batch_date = datetime.datetime.now()
  batch_date = batch_date.strftime('%d.%m.%Y')
  df_to_file = pd.DataFrame()

  if pages == -1:
    return

  for page in range(1, pages +1):
    url = '{}&atype=C&search_id=1ff7fqyhuvn&page={}'.format(pre_url,page)
    r = urlopen(url, context=ssl.create_default_context(cafile=certifi.where()))
    soup = BeautifulSoup(r, "html.parser")
    content = soup.find_all("div", {"class": "ListItem_wrapper__J_a_C"})

    for offer in content:
      offer = str(offer)
      offer_tech_pattern = re.compile('(?<=class=\"VehicleDetailTable_item__koEV4\">)(.*?)(?=<\/span>)')
      offer_tech_raw = re.findall(offer_tech_pattern,offer)
      mileage = offer_tech_raw[0]

      production_year = str(offer_tech_raw[1])
      power = offer_tech_raw[2]
      fuel_type = offer_tech_raw[-1]

      price_pattern = re.compile('(?<=\"\'>)(.*?)(?=\.-<\/p><\/div>)')
      price = re.findall(price_pattern,offer)
      
      currency = ''
      if '€' in price[0]:
        currency = 'EUR'
      else:
        continue
      price = format_price(price[0])
      
      production_year = offer_tech_raw[1]

      if r'/' not in production_year:
        continue

      production_year = format_year(production_year)
      mileage, mileage_unit = format_mileage(mileage)
      power, power_unit = format_power(offer_tech_raw[2])
      fuel_type = offer_tech_raw[-1]
    

      data = {
            'brand' : [brand],
            'model' : [model],
            'production_date' : [production_year],
            'price' : [price],
            'currency':[currency],
            'mileage' : [mileage],
            'mileage_unit' : [mileage_unit],
            'power' : [power],
            'engine_capacity_unit': [power_unit],
            'fuel_type':[fuel_type],
            'scrape_date':[batch_date]
            }

      df = pd.DataFrame(data)
      df_to_file = pd.concat([df, df_to_file], axis=0)

  df_to_file.to_csv('autoscout24.csv', mode='a', index=False, header=False)



def recive_link(pre_url):
  pool = ThreadPool()
  prev = 0
  for year in range(0, 1500000,500):
    url = f'{pre_url}?kmfrom={prev}&kmto={year}'
    prev = year
    pool.apply_async(scrape_autoscout, (url,))

  pool.close()
  pool.join()

if __name__ == '__main__':
  df = pd.read_csv('links.csv', sep=";",dtype=str)
  link_list = df.iloc[:, 0].tolist()
  pool = ThreadPool()

  with concurrent.futures.ProcessPoolExecutor() as executor:
      result = list(tqdm(executor.map(recive_link, link_list), total=len(link_list)))
