from selenium import webdriver
from selenium.webdriver.common.by import By
from typing import List
import time
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)


class RolexScraper:
    def __init__(self, url):
        self.url = url
        self.driver = webdriver.Chrome()
        self.driver.get(self.url)
        self.data: List[str] = []
        time.sleep(10)

    def scrape(self):
        while True:
            try:
                load_more_button = self.driver.find_element(
                    By.CLASS_NAME, "products-styles__LoadMoreButton-sc-1qarpt3-4"
                )
                load_more_button.click()
                time.sleep(5)
            except:
                break

        elements = self.driver.find_elements(
            By.CLASS_NAME, "custom-product-card__ProductBrand"
        )
        prices = self.driver.find_elements(
            By.CLASS_NAME, "syte-ts-original-price-value"
        )
        h2_elements = self.driver.find_elements(By.TAG_NAME, "h2")

        titles = [element.text for element in elements]
        prices = [element.text for element in prices]
        marks = [element.text for element in h2_elements]

        for title, price, mark in zip(titles, prices, marks):
            self.data.append({"Watch_Name": title, "Price": price, "Watch_Mark": mark})
            logging.info(f"Name: {title}, Price: {price}, Mark: {mark}")

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

    def close(self):
        self.driver.quit()


if __name__ == "__main__":
    scraper = RolexScraper(
        "https://www.worldofwatches.com/syte/search/result?query=rolex&from=0&filters=%7B%7D&filtersDisplayNames=%7B%7D"
    )
    scraper.scrape()
    time.sleep(15)
    scraper.save_to_csv("rolex_watches.csv", include_mark=True)
    scraper.close()
