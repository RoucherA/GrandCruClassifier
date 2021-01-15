from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool
import os
import shutil
import time
import requests
import re
import json
import glob

# Our base URL includes the term 'france', which precises we want the french prices for the wines
BASE_URL = "https://www.idealwine.com/uk/wine-prices/{}-grand-cru-classe.jsp"

session = requests.Session()
HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36"
    )
}
DATA_DIR = "data"
FILENAME = "idealwine-data"

premiers_grands_crus_classes = ['Château Lafite Rothschild Pauillac', 'Château Latour Pauillac', 'Château Mouton Rothschild Pauillac', 'Château Margaux', 'Château Haut-Brion Pessac-léognan']
for i in range(len(premiers_grands_crus_classes)):
    premiers_grands_crus_classes[i] = premiers_grands_crus_classes[i].replace(' ', '-')
print(premiers_grands_crus_classes)

label = '(Premier Grand Cru Classé)'

class Scraper:
    """Scraper for iDealwine.com to collect wine reviews. Adapted from @zackthoutt webscraper."""

    def __init__(
        self, vineyard_list, min_vintage, clear_old_data=True
    ):
        self.clear_old_data = clear_old_data
        self.session = requests.Session()
        self.start_time = time.time()
        self.vineyard_list = vineyard_list
        self.min_vintage = min_vintage

    def scrape_site(self):
        if self.clear_old_data:
            self.clear_data_dir()

        for vineyard in self.vineyard_list:
            print('Target vineyard: ', vineyard)
            self.scrape_vineyard(BASE_URL.format(vineyard))
        print("Scrape finished...")
        self.condense_data()

    def scrape_vineyard(self, page_url, retry_count=0):
        scrape_data = []
        try:
            print(page_url)
            response = self.session.get(page_url, headers=HEADERS)

        except:
            retry_count += 1
            if retry_count <= 3:
                self.session = requests.Session()
                self.scrape_vineyard(page_url, retry_count)
            else:
                raise

        soup = BeautifulSoup(response.content, "html.parser")
        # With the precision of our request, the target wine normally shows up first, thus we get it with index 0
        wine_link = soup.find_all("table", {"id": "tbResult"})[0].a['href']
        
        wine_url = 'https://www.idealwine.com' + wine_link
        print(wine_url)
        
        scrape_data = []
        year=2017
        while year >= self.min_vintage:
            wine_response = self.session.get(wine_url.replace('2017', str(year)), headers=HEADERS)
            wine_soup = BeautifulSoup(wine_response.content, "html.parser")
            scrape_data.append(self.parse_vineyard(wine_soup))
            year -= 1
        self.save_data(scrape_data)

    def parse_vineyard(self, wine_soup):
        price = int(wine_soup.find_all('article', {'class':'indice-table'})[0].text.split('€')[0])
        print(price)
        
        review_data = {
            "price": price
        }

        return review_data


    def save_data(self, data):
        filename = "{}/{}_{}.json".format(DATA_DIR, FILENAME, time.time())
        try:
            os.makedirs(DATA_DIR)
        except OSError:
            pass
        with open(filename, "w") as fout:
            json.dump(data, fout)

    def clear_all_data(self):
        self.clear_data_dir()
        self.clear_output_data()

    def clear_data_dir(self):
        try:
            shutil.rmtree(DATA_DIR)
        except FileNotFoundError:
            pass

    def clear_output_data(self):
        try:
            os.remove("{}.json".format(FILENAME))
        except FileNotFoundError:
            pass

    def condense_data(self):
        print("Condensing Data...")
        condensed_data = []
        all_files = glob.glob("{}/*.json".format(DATA_DIR))
        for file in all_files:
            with open(file, "rb") as fin:
                condensed_data += json.load(fin)
        print(len(condensed_data))
        filename = "{}.json".format(FILENAME)
        with open(filename, "w") as fout:
            json.dump(condensed_data, fout)

    def update_scrape_status(self):
        elapsed_time = round(time.time() - self.start_time, 2)
        time_remaining = round(
            (self.estimated_total_reviews - self.cross_process_review_count)
            * (self.cross_process_review_count / elapsed_time),
            2,
        )
        print(
            "{4} page {0}/{1} reviews | {2} sec elapsed | {3} sec remaining\r".format(
                self.cross_process_review_count,
                self.estimated_total_reviews,
                elapsed_time,
                time_remaining,
                self.page_scraped,
            )
        )


class ReviewFormatException(Exception):
    """Exception when the format of a review page is not understood by the scraper"""

    def __init__(self, message):
        self.message = message
        super(Exception, self).__init__(message)


if __name__ == "__main__":

    # Total review results on their site are conflicting, hardcode as the max tested value for now
    winmag_scraper = Scraper(
        premiers_grands_crus_classes,
        1990,
        clear_old_data=True,
    )

    # Step 1: scrape data
    winmag_scraper.scrape_site()

    # Step 2: condense data
    winmag_scraper.condense_data()