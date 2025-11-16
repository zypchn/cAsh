import re
import pandas as pd
import json
import requests
import time
import asyncio
from bs4 import BeautifulSoup

SM_urls = [
    "https://www.pricecharting.com/console/pokemon-sun-&-moon",
    "https://www.pricecharting.com/console/pokemon-guardians-rising",
    "https://www.pricecharting.com/console/pokemon-burning-shadows",
    "https://www.pricecharting.com/console/pokemon-shining-legends",
    "https://www.pricecharting.com/console/pokemon-crimson-invasion",
    "https://www.pricecharting.com/console/pokemon-ultra-prism",
    "https://www.pricecharting.com/console/pokemon-forbidden-light",
    "https://www.pricecharting.com/console/pokemon-celestial-storm",
    "https://www.pricecharting.com/console/pokemon-dragon-majesty",
    "https://www.pricecharting.com/console/pokemon-lost-thunder",
    "https://www.pricecharting.com/console/pokemon-team-up",
    "https://www.pricecharting.com/console/pokemon-detective-pikachu",
    "https://www.pricecharting.com/console/pokemon-unbroken-bonds",
    "https://www.pricecharting.com/console/pokemon-unified-minds",
    "https://www.pricecharting.com/console/pokemon-hidden-fates",
    "https://www.pricecharting.com/console/pokemon-cosmic-eclipse"
]

SWSH_urls = [
    "https://www.pricecharting.com/console/pokemon-sword-&-shield",
    "https://www.pricecharting.com/console/pokemon-rebel-clash",
    "https://www.pricecharting.com/console/pokemon-darkness-ablaze",
    "https://www.pricecharting.com/console/pokemon-champion%27s-path",
    "https://www.pricecharting.com/console/pokemon-vivid-voltage",
    "https://www.pricecharting.com/console/pokemon-shining-fates",
    "https://www.pricecharting.com/console/pokemon-battle-styles",
    "https://www.pricecharting.com/console/pokemon-chilling-reign",
    "https://www.pricecharting.com/console/pokemon-evolving-skies",
    "https://www.pricecharting.com/console/pokemon-celebrations",
    "https://www.pricecharting.com/console/pokemon-fusion-strike",
    "https://www.pricecharting.com/console/pokemon-brilliant-stars",
    "https://www.pricecharting.com/console/pokemon-astral-radiance",
    "https://www.pricecharting.com/console/pokemon-go",
    "https://www.pricecharting.com/console/pokemon-lost-origin",
    "https://www.pricecharting.com/console/pokemon-silver-tempest",
    "https://www.pricecharting.com/console/pokemon-crown-zenith"
]

SV_urls = [
    "https://www.pricecharting.com/console/pokemon-scarlet-&-violet",
    "https://www.pricecharting.com/console/pokemon-paldea-evolved",
    "https://www.pricecharting.com/console/pokemon-obsidian-flames",
    "https://www.pricecharting.com/console/pokemon-scarlet-&-violet-151",
    "https://www.pricecharting.com/console/pokemon-paradox-rift",
    "https://www.pricecharting.com/console/pokemon-paldean-fates",
    "https://www.pricecharting.com/console/pokemon-temporal-forces",
    "https://www.pricecharting.com/console/pokemon-twilight-masquerade",
    "https://www.pricecharting.com/console/pokemon-shrouded-fable",
    "https://www.pricecharting.com/console/pokemon-stellar-crown",
    "https://www.pricecharting.com/console/pokemon-surging-sparks",
    "https://www.pricecharting.com/console/pokemon-prismatic-evolutions",
    "https://www.pricecharting.com/console/pokemon-journey-together",
    "https://www.pricecharting.com/console/pokemon-destined-rivals",
    "https://www.pricecharting.com/console/pokemon-black-bolt",
    "https://www.pricecharting.com/console/pokemon-white-flare"
]


# scrapes the first 50 products from an URL
def scrape_prod_urls(url):
    time.sleep(10)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    prod_lists = soup.find("tbody")
    prod_items = prod_lists.find_all("tr")
    all_urls = [item.a["href"] for item in prod_items if item.a and item.a.get("href")]
    return all_urls

def is_card(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    return str(soup.title).find("#") != -1

# scrapes the price data as time series from an URL
def scrape_price(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    prod_name = soup.title.text.split("|")[0].split("Prices")[0].strip().replace(" ", "_")
    data_script = soup.find('script', string=re.compile("VGPC.chart_data = {"))
    data = json.loads(data_script.text.split('VGPC.chart_data = ')[1].split(' VGPC.product = {')[0].split(';')[0].strip())
    return prod_name, data

# combines metadata and price data and transforms to DataFrame
def transform_to_df(prod_name, data, set_name):
    df_used = pd.DataFrame(data['used'], columns = ['Date_time', 'Price'])
    df_graded = pd.DataFrame(data['graded'], columns = ['Date_time', 'Price'])
    df_prices = pd.merge(df_used, df_graded, on="Date_time")
    df_prices = df_prices.rename(columns={"Price_x": "used_price", "Price_y": "graded_price"})
    df_prices['Date_time'] = pd.to_datetime(df_prices['Date_time'], unit="ms")
    df_prices = df_prices.set_index(['Date_time'])
    df_prices["prod_name"] = prod_name
    df_prices["set_name"] = set_name
    return df_prices


poke_prods = pd.DataFrame()

## CHANGE URL ##
for set_url in SM_urls:
    set_name = set_url.split("/")[-1]
    all_urls = scrape_prod_urls(set_url)
    for url in all_urls:
        try:
            if is_card(url):
                prod_name, data = scrape_price(url)
                prod_df = transform_to_df(prod_name, data, set_name)
                poke_prods = pd.concat([poke_prods, prod_df])
        except Exception as e:
            pass
    print(f"data scraping for '{set_name}' has finished")

## CHANGE NAME ##
poke_prods.to_csv("SM_raw.csv")
