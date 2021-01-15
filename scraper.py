from bs4 import BeautifulSoup
import requests
import pandas as pd

# Our base URL includes the term 'france', which precises we want the french prices for the wines
BASE_URL = "https://www.idealwine.com/uk/wine-prices/{}-1er-grand-cru-classe.jsp"

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
        self, vineyard_list, min_vintage
    ):
        self.session = requests.Session()
        self.vineyard_list = vineyard_list
        self.min_vintage = min_vintage
        self.result = pd.DataFrame(0, index=range(2020,min_vintage-1, -1), columns=vineyard_list)

    def scrape_site(self):

        columns=[]
        for vineyard in self.vineyard_list:
            print('Target vineyard: ', vineyard)
            self.scrape_vineyard(vineyard)
        print("Scrape finished...")
        return self.result

    def scrape_vineyard(self, vineyard, retry_count=0):
        page_url = BASE_URL.format(vineyard)
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
            self.result.loc[year, vineyard]=self.parse_vineyard(wine_soup)
            year -= 1
        return

    def parse_vineyard(self, wine_soup):
        try:
            price = int(wine_soup.find_all('article', {'class':'indice-table'})[0].text.split('€')[0])
        except:
            price=0
        return price

    def write_table(self, data):
        exit()
        return


if __name__ == "__main__":
    # Total review results on their site are conflicting, hardcode as the max tested value for now
    scraper = Scraper(
        premiers_grands_crus_classes,
        2010
    )

    print(scraper.scrape_site().head(20).to_string())

