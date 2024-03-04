from typing import List
from bs4 import BeautifulSoup
from base_scraper import BaseScraper


class LuxuryWatchesScraper(BaseScraper):
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
        Initializes the LuxuryWatchesScraper class.

        Parameters:
        ----------
        None
        """
        super().__init__()

    def extract_data(self, response):
        """
        Extracts data from the page.

        Parameters:
        ----------
        None

        Returns:
        -------
        None
        """
        soup = BeautifulSoup(response.content, "html.parser")
        titles = soup.find_all(
            "p", class_="name product-title woocommerce-loop-product__title"
        )
        prices = soup.find_all("span", class_="woocommerce-Price-currencySymbol")
        return titles, prices


if __name__ == "__main__":
    # Main function to initiate scraping and saving
    scraper = LuxuryWatchesScraper()
    base_url = "https://luxurywatchesusa.com/page/{}/?s=patek&post_type=product"
    scraper.scrape_all_pages(base_url)
    scraper.save_to_csv(filename="luxury_watches.csv", include_mark=False)
