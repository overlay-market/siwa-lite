import requests
from bs4 import BeautifulSoup


class Chrono24Scraper:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

    def scrape_names_from_page(self, url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            titles = soup.find_all(
                "div", class_="text-sm text-sm-md text-bold text-ellipsis"
            )
            prices = soup.find_all("span", class_="currency")
            for i in range(len(titles)):
                title = titles[i].text.strip()
                price = prices[i].next_sibling.strip()
                print(f"Name: {title}, Price: {price}$")
        else:
            print("Failed to retrieve the webpage. Status code:", response.status_code)

    def scrape_all_pages(self, base_url, start_page=1):
        page_number = start_page
        while True:
            url = base_url.format(page_number)
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                print(f"Scraping page {page_number}")
                self.scrape_names_from_page(url)
                page_number += 1
            else:
                print(
                    "Failed to retrieve the webpage. Status code:", response.status_code
                )
                break  # Stop scraping if there's an issue fetching the page


if __name__ == "__main__":
    scraper = Chrono24Scraper()
    base_url = "https://www.chrono24.com/rolex/index-{}.htm?query=Rolex"
    scraper.scrape_all_pages(base_url)
