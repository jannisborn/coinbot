import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://de.wikipedia.org/wiki/2-Euro-Gedenkmünzen"


def get_wikitable_and_imgs():
    """Takes a wikipedia URL with a table in the webpage and returns a list of dataframes from the webpage.
    This function will scrape every table in the wikipedia URL if no table_numbers are specified.

    Returns
    -------
    A list of Pandas Dataframes if multiple tables are scraped.
    """
    r = requests.get(URL)
    if r.status_code == 404:
        assert False, "<Response [404]>\nURL not valid."
    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})
    dfs = pd.read_html(str(tables))
    # There are 31 tables and 35 DFs because some years contain subtables.
    # We have to align both lists
    table_iter = iter(tables)
    tables = [next(table_iter) if "Land" in df.columns else [] for df in dfs]
    for i, (table, df) in enumerate(zip(tables, dfs)):
        if i < 2 or table == []:
            continue
        tablerows = table.findAll("tr")
        assert len(tablerows) == len(df) + 1
        image_urls = []
        for j, (tablerow, (_, dfrow)) in enumerate(zip(tablerows[1:], df.iterrows())):
            img_tag = tablerow.find("img")
            if img_tag and "src" in img_tag.attrs:
                img_url = img_tag["src"]
                if img_url.startswith("//"):
                    img_url = "https:" + img_url  # Ensure the URL is complete
                image_urls.append(img_url)
            else:
                image_urls.append("")

        df["image_url"] = image_urls
    return dfs
