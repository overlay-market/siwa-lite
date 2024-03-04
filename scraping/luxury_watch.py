import logging
from typing import List
import requests
from bs4 import BeautifulSoup
import pandas as pd


class LuxuryWatchesScraper:
    """
    A web scraper for extracting watch names and prices from Chrono24.

    Attributes:
    ----------
    User_Agent : str
        User agent string for making requests.
    headers : dict
        Headers to be used in HTTP requests.
    data : List[str]
        List to store scraped data.
    """

    User_Agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    def __init__(self) -> None:
        """
        Initializes the Chrono24Scraper class.

        Parameters:
        ----------
        None
        """
        self.headers: dict = {"User-Agent": self.User_Agent}
        self.data: List[str] = []
        logging.basicConfig(level=logging.INFO)

    def scrape_names_from_page(self, url: str) -> None:
        """
        Scrapes watch names and prices from a single page.

        Parameters:
        ----------
        url : str
            The URL of the page to scrape.

        Returns:
        -------
        None
        """

        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            titles = soup.find_all(
                'p', class_='name product-title woocommerce-loop-product__title')
            prices = soup.find_all(
                'span', class_='woocommerce-Price-currencySymbol')

            # Ensure both lists are of equal length
            min_length = min(len(titles), len(prices))

            for i in range(min_length):
                title = titles[i].text.strip()
                price = prices[i].next_sibling.strip()
                self.data.append(
                    {"Watch_Name": title, "Price": price}
                )
                logging.info(f"Name: {title}, Price: {price}$")
        else:
            logging.error(
                "Failed to retrieve the webpage. Status code: %d", response.status_code
            )

    def scrape_all_pages(
        self, base_url: str, start_page: int = 1, num_pages: int = 7
    ) -> None:
        """
        Scrapes data from multiple pages.

        Parameters:
        ----------
        base_url : str
            The base URL of the website to scrape.
        start_page : int, optional
            The starting page number. Default is 1.
        num_pages : int, optional
            The number of pages to scrape. Default is 5.

        Returns:
        -------
        None
        """

        page_number = start_page
        while page_number <= num_pages:
            url = base_url.format(page_number)
            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code == 200:
                    logging.info(f"Scraping page {page_number}")
                    self.scrape_names_from_page(url)
                    page_number += 1
                else:
                    logging.error(
                        "Failed to retrieve the webpage. Status code: %d",
                        response.status_code,
                    )
                    break
            except requests.exceptions.RequestException as e:
                logging.error(
                    "An error occurred while making the request: %s", e)
                break

    def save_to_csv(self, filename: str = "luxury_watches.csv") -> None:
        """
        Saves scraped data to a CSV file.

        Parameters:
        ----------
        filename : str, optional
            The name of the CSV file. Default is "chrono24_data.csv".

        Returns:
        -------
        None
        """
        df = pd.DataFrame(self.data, columns=["Watch_Name", "Price"])
        df.to_csv(filename, index=True)
        logging.info(f"Data saved to {filename}")


if __name__ == "__main__":
    # Main function to initiate scraping and saving
    scraper = LuxuryWatchesScraper()
    base_url = "https://luxurywatchesusa.com/page/{}/?s=rolex&post_type=product"
    scraper.scrape_all_pages(base_url)
    scraper.save_to_csv()
