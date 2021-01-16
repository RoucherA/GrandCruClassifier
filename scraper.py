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

# Let us first create a list of all grands crus from the 1855 ranking of Medoc
premiers_grands_crus_classes_rouges = [
    'Château Lafite Rothschild Pauillac',
    'Château Latour Pauillac',
    'Château Mouton Rothschild Pauillac',
    'Château Margaux',
    'Château Haut-Brion Pessac-léognan'
    ]
for i in range(len(premiers_grands_crus_classes)):
    premiers_grands_crus_classes[i] = premiers_grands_crus_classes[i].replace(' ', '-')+'-1er-cru'

deuxiemes_grands_crus_classes_rouges = [
    'Château Brane-Cantenac Margaux',
    'Château Durfort-Vivens Margaux',
    'Château Lascombes Margaux',
    'Château Rauzan-Gassies Margaux',
    'Château Rauzan-Ségla Margaux',
    'Château Pichon-Longueville Baron Pauillac',
    'Château Pichon-Longueville Comtesse de Lalande Pauillac',
    "Château Cos d'Estournel Saint-Estèphe",
    'Château Montrose Saint-Estèphe',
    'Château Ducru-Beaucaillou Saint-Julien',
    'Château Gruaud Larose Saint-Julien',
    'Château Léoville Barton Saint-Julien',
    'Château Léoville Las Cases Saint-Julien',
    'Château Léoville Poyferré Saint-Julien'
    ]
for i in range(len(deuxiemes_grands_crus_classes)):
    deuxiemes_grands_crus_classes[i] = deuxiemes_grands_crus_classes[i].replace(' ', '-')+'-2eme-cru'

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
        
        # Initialize vintage by the highest, i.e. most recent vintage referenced on the website
        wine_response = self.session.get(wine_url, headers=HEADERS)
        wine_soup = BeautifulSoup(wine_response.content, "html.parser")
        max_vintage_str = wine_soup.find_all('a', {'class':'ola selected-vintage'})[0].text
        vintage = int(max_vintage_str)

        while vintage >= self.min_vintage:
            print(max_vintage_str, vintage, wine_url.replace(max_vintage_str, str(vintage)))
            wine_response = self.session.get(wine_url.replace(max_vintage_str, str(vintage)), headers=HEADERS)
            wine_soup = BeautifulSoup(wine_response.content, "html.parser")
            self.result.loc[vintage, vineyard]=self.parse_vineyard(wine_soup)
            vintage -= 1
        return

    def parse_vineyard(self, wine_soup):
        try:
            price = int(wine_soup.find_all('article', {'class':'indice-table'})[0].text.split('€')[0].replace(' ',''))
        except:
            price=0
        return price

    def write_table(self, data):
        exit()
        return


if __name__ == "__main__":
    # Total review results on their site are conflicting, hardcode as the max tested value for now
    scraper = Scraper(
        premiers_grands_crus_classes + deuxiemes_grands_crus_classes,
        2010
    )
    table = scraper.scrape_site()
    print(table.head(20).to_string())
    table.to_excel('table.xls', encoding='utf-16')

