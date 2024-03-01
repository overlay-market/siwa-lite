import logging
from typing import List
import requests
from bs4 import BeautifulSoup
import pandas as pd


class Chrono24Scraper:
    User_Agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"

    def __init__(self) -> None:
        self.headers: dict = {
            "User-Agent": self.User_Agent
        }
        self.data: List[str] = []
        logging.basicConfig(level=logging.INFO)

    def scrape_names_from_page(self, url: str) -> None:
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            titles = soup.find_all(
                "div", class_="text-sm text-sm-md text-bold text-ellipsis"
            )
            prices = soup.find_all("span", class_="currency")

            if len(titles) != len(prices):
                logging.error("Number of titles and prices do not match")
                return
            for i in range(len(titles)):
                title = titles[i].text.strip()
                price = prices[i].next_sibling.strip()
                self.data.append({"Name": title, "Price": price})
                logging.info(f"Name: {title}, Price: {price}$")
        else:
            logging.error("Failed to retrieve the webpage. Status code: %d", response.status_code)

    def scrape_all_pages(self, base_url: str, start_page: int = 1) -> None:
        page_number = start_page
        while page_number <= 5:
            url = base_url.format(page_number)
            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code == 200:
                    logging.info(f"Scraping page {page_number}")
                    self.scrape_names_from_page(url)
                    page_number += 1
                else:
                    logging.error("Failed to retrieve the webpage. Status code: %d", response.status_code)
                    break
            except requests.exceptions.RequestException as e:
                logging.error("An error occurred while making the request: %s", e)
                break

    def save_to_csv(self, filename: str = "chrono24_data.csv") -> None:
        df = pd.DataFrame(self.data, columns=["Name", "Price"])
        df.to_csv(filename, index=False)
        logging.info(f"Data saved to {filename}")


if __name__ == "__main__":
    scraper = Chrono24Scraper()
    base_url = "https://www.chrono24.com/rolex/index-{}.htm?query=Rolex"
    scraper.scrape_all_pages(base_url)
    scraper.save_to_csv()
