# Importing necessary modules
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import logging
from base_scraper import BaseScraper


class WorldWatches(BaseScraper):
    """
    A web scraper for extracting watch names and prices from World of Watches.

    Attributes:
    ----------
    User_Agent : str
        User agent string for making requests.
    """

    User_Agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    )

    def __init__(self, url):
        """
        Initializes the WorldWatches class.

        Parameters:
        ----------
        url : str
            The URL of the website to be scraped.
        """
        super().__init__()
        self.url = url
        self.driver = webdriver.Chrome()
        self.driver.get(self.url)
        time.sleep(10)

    def scrape(self):
        """
        Scrapes the website for watch names, prices, and marks.

        This method iterates through the website, clicking on the "Load More" button
        until there are no more products to load. Then, it extracts watch names,
        prices, and marks from the loaded page and appends them to the data attribute
        of the WorldWatches class.

        Returns:
        ----------
        None
        """
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
        marks = self.driver.find_elements(By.TAG_NAME, "h2")

        titles = [element.text for element in elements]
        prices = [element.text for element in prices]
        marks = [element.text for element in marks]

        for title, price, mark in zip(titles, prices, marks):
            self.data.append({"Watch_Name": title, "Price": price, "Watch_Mark": mark})
            logging.info(f"Name: {title}, Price: {price}, Mark: {mark}")

    def close(self):
        """
        Closes the webdriver.
        """
        self.driver.quit()


if __name__ == "__main__":
    scraper = WorldWatches(
        "https://www.worldofwatches.com/syte/search/result?query=rolex&from=0&filters=%7B%7D&filtersDisplayNames=%7B%7D"
    )
    scraper.scrape()
    time.sleep(15)
    scraper.save_to_csv("world_of_watches.csv", include_mark=True)
    scraper.close()
