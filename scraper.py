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
BASE_URL = "https://www.wine-searcher.com/find/{}/1/france"

session = requests.Session()
HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36"
    )
}
DATA_DIR = "data"
FILENAME = "vivino-data"

premiers_grands_crus_classes = ['chateau','Château Lafite Rothschild Pauillac', 'Château Latour Pauillac', 'Château Mouton Rothschild Pauillac', 'Château Margaux', 'Château Haut-Brion Pessac-léognan']
label = '(Premier Grand Cru Classé)'

class Scraper:
    """Scraper for Vivino.com to collect wine reviews. Adapted from @zackthoutt webscraper."""

    def __init__(
        self, vineyard_list, clear_old_data=True
    ):
        self.clear_old_data = clear_old_data
        self.session = requests.Session()
        self.start_time = time.time()
        self.vineyard_list = vineyard_list

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

        print(soup)
        labels = soup.find_all('span', {'class': 'text-truncate-2lines'})
        print(labels)
        exit()
        # Find all items in the global list
        wines = soup.find_all("span", {"class": "header-smaller text-block wine-card__name"})


        print(wines)
        exit()
        # With the precision of our request, the target wine normally shows up first
        wine_link = 'https://www.vivino.com' + wines[0].find('a')['href']

        wine_response = self.session.get(wine_link, headers=HEADERS)

        wine_soup = BeautifulSoup(wine_response.content, "html.parser")
        scrape_data = self.parse_vineyard(wine_soup)
        self.save_data(scrape_data)


    def parse_vineyard(self, wine_soup):
        winery = wine_soup.find_all('a', {'class':'winery'})[0].text
        vintage = wine_soup.find_all('span', {'class':'vintage'})[0].text
        print(winery.replace('\n', '')+vintage)

        print('Waiting for page load...')
        time.sleep(3.0)
        prices = wine_soup.find_all('a', {'class':'anchor__anchor--3DOSm vintageList__showAll--7cLR3 anchor__vivinoLink--29E1-'})
        print(prices)
        exit()
        review_data = {
            "points": points,
            "title": title,
            "description": description,
            "taster_name": taster_name,
            "taster_twitter_handle": taster_twitter_handle,
            "taster_photo": taster_photo,
            "price": price,
            "designation": designation,
            "variety": variety,
            "region_1": region_1,
            "region_2": region_2,
            "province": province,
            "country": country,
            "winery": winery,
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
        clear_old_data=True,
    )

    # Step 1: scrape data
    winmag_scraper.scrape_site()

    # Step 2: condense data
    winmag_scraper.condense_data()