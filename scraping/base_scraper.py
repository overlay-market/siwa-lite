import logging
from typing import List
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import time


class BaseScraper:
    """
    A base class for web scrapers.

    Attributes:
    ----------
    headers : dict
        Headers to be used in HTTP requests.
    data : List[str]
        List to store scraped data.
    """

    def __init__(self) -> None:
        """
        Initializes the BaseScraper class.

        Parameters:
        ----------
        None
        """
        self.headers: dict = {"User-Agent": self.User_Agent}
        self.data: List[str] = []
        logging.basicConfig(level=logging.INFO)
        retry_strategy = Retry(total=3, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

    def scrape_names_from_page(cls, url) -> None:
        """
        Scrapes data from a single page.

        Parameters:
        ----------
        url : str
            The URL of the page to scrape.
        Returns:
        -------
        None
        """
        response = cls.http.get(url, headers=cls.headers)
        if response.status_code == 200:
            extract = cls.extract_data(response)

            min_length = min(len(extract[0]), len(extract[1]))

            for i in range(min_length):
                title = extract[0][i].text.strip()
                price = extract[1][i].next_sibling.strip()
                mark = extract[2][i].text.strip() if len(extract) > 2 else None
                cls.data.append(
                    {"Watch_Name": title, "Price": price, "Watch_Mark": mark}
                )
                logging.info(f"Name: {title}, Price: {price}, Mark: {mark}")

        else:
            logging.error(
                "Failed to retrieve the webpage. Status code: %d", response.status_code
            )

        time.sleep(10)

    def scrape_all_pages(
        self, base_url: str, start_page: int = 1, num_pages: int = 6
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
        while True:
            url = base_url.format(page_number)
            try:
                response = self.http.get(url, headers=self.headers, verify=False)
                if response.status_code == 200:
                    logging.info(f"Scraping page {page_number}")
                    self.scrape_names_from_page(url)
                    page_number += 1
                    time.sleep(5)
                else:
                    logging.error(
                        "Failed to retrieve the webpage. Status code: %d",
                        response.status_code,
                    )
                    break
            except requests.exceptions.RequestException as e:
                logging.error("An error occurred while making the request: %s", e)
                break

    def save_to_csv(self, filename: str, include_mark: bool = True) -> None:
        """
        Saves scraped data to a CSV file.

        Parameters:
        ----------
        filename : str
            The name of the CSV file.
        include_mark : bool, optional
            Whether to include the "Watch_Mark" column, by default True.

        Returns:
        -------
        None
        """
        columns = ["Watch_Name", "Price"]
        if include_mark:
            columns.append("Watch_Mark")

        df = pd.DataFrame(self.data, columns=columns)
        df.to_csv(filename, index=True)
        logging.info(f"Data saved to {filename}")
