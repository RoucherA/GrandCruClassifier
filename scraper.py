from bs4 import BeautifulSoup
import requests
import pandas as pd
import concurrent.futures
import time
from tqdm import tqdm

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36"
    )
}
BASE_URL = 'https://www.idealwine.com'
SEARCH_URL = BASE_URL+"/uk/wine-prices/{}.jsp"

# Let us first create a list of all grands crus from the 1855 ranking of Medoc
premiers_grands_crus_classes_1855 = [
    'Château Lafite Rothschild Pauillac',
    'Château Latour Pauillac',
    'Château Mouton Rothschild Pauillac',
    'Château Margaux',
    'Château Haut-Brion Pessac-léognan'
    ]
deuxiemes_grands_crus_classes_1855 = [
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
premiers_grands_crus_classes_st_emilion=[
    'Château Ausone',
    'Chateau Cheval Blanc',
    'Chateau Pavie',
    'Chateau Belair-Monange',
    'Chateau Figeac',
    'Chateau Magdelaine',
    'Chateau TrotteVieille',
    'Chateau Beausejour Duffau-Lagarrosse',
    'Chateau Canon',
    'Clos Fourtet',
    'Chateau la Gaffeliere-Naudes'
]


class Vineyard:
    def __init__(self, name, category, search_suffix):
        self.name = name
        self.category = category
        self.search_suffix = search_suffix

VINEYARD_LIST = []
for vineyard_name in premiers_grands_crus_classes_1855:
    VINEYARD_LIST.append(Vineyard(vineyard_name, 'Premier cru 1855','-1er-grand-cru-classe'))
for vineyard_name in deuxiemes_grands_crus_classes_1855:
    VINEYARD_LIST.append(Vineyard(vineyard_name, 'Deuxieme cru 1855','-2eme-grand-cru-classe'))
for vineyard_name in premiers_grands_crus_classes_st_emilion:
    VINEYARD_LIST.append(Vineyard(vineyard_name, 'Premier cru Saint Emilion','-Saint-Emilion-1er-Classe'))

class Scraper:
    """Scraper for iDealwine.com to collect wine reviews. Adapted from @zackthoutt webscraper."""

    def __init__(
        self, vineyard_list, min_vintage, num_workers
    ):
        self.start_time = time.time()
        self.session = requests.Session()
        self.vineyard_list = vineyard_list
        self.result = pd.DataFrame(0, index=[vineyard.name for vineyard in self.vineyard_list], columns=['Category']+list(range(min_vintage, 2020, 1)))
        self.min_vintage = min_vintage
        self.num_workers = num_workers

    def scrape_site(self):
        print('Beginning to scrape iDealwine...')
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            res = list(tqdm(executor.map(self.scrape_vineyard, self.vineyard_list), total=len(self.vineyard_list)))
        print("Scraping finished in ", int(time.time() - self.start_time), 's.\n')
        return self.result 

    def scrape_vineyard(self, vineyard, retry_count=0):
        vineyard_search_term = vineyard.name.replace(' ', '-') + vineyard.search_suffix
        page_url = SEARCH_URL.format(vineyard_search_term)
        try:
            response = self.session.get(page_url, headers=HEADERS)

        except:
            retry_count += 1
            if retry_count <= 3:
                self.session = requests.Session()
                self.scrape_vineyard(page_url, retry_count)
            else:
                raise

        soup = BeautifulSoup(response.content, "html.parser")
        # With the precision of our request, the target wine normally shows up first, thus we obtain it with index 0
        wine_link = soup.find_all("table", {"id": "tbResult"})[0].a['href']
        wine_url = BASE_URL + wine_link
        
        # Initialize vintage by the highest, i.e. most recent vintage referenced on the website
        wine_response = self.session.get(wine_url, headers=HEADERS)
        wine_soup = BeautifulSoup(wine_response.content, "html.parser")
        max_vintage_str = wine_soup.find_all('a', {'class':'ola selected-vintage'})[0].text
        max_vintage = int(max_vintage_str)

        for vintage in range(max_vintage, self.min_vintage-1, -1):
            wine_response = self.session.get(wine_url.replace(max_vintage_str, str(vintage)), headers=HEADERS)
            wine_soup = BeautifulSoup(wine_response.content, "html.parser")
            self.result.loc[vineyard.name, vintage] = self.parse_vineyard(wine_soup)
            self.result.loc[vineyard.name, 'Category'] = vineyard.category

        return

    def parse_vineyard(self, wine_soup):
        try:
            price = int(wine_soup.find_all('article', {'class':'indice-table'})[0].text.split('€')[0].replace(' ',''))
        except:
            price=0
        return price

if __name__ == "__main__":
    # Total review results on their site are conflicting, hardcode as the max tested value for now
    scraper = Scraper(
        VINEYARD_LIST,
        1950,
        num_workers=10
    )
    table = scraper.scrape_site()
    print(table.iloc[:5,:3].to_string())
    table.to_excel('data/prices.xlsx', encoding='utf-16')

