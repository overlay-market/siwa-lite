from typing import List
from bs4 import BeautifulSoup
from base_scraper import BaseScraper


class AviandcoScraper(BaseScraper):
    """
    A web scraper for extracting watch names and prices from Aviandco.

    Attributes:
    ----------
    User_Agent : str
        User agent string for making requests.
    """

    User_Agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
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
        response : requests.Response
            The HTTP response object.

        Returns:
        -------
        Tuple[List[Tag], List[Tag], List[Tag]]
            A tuple containing lists of Beautiful Soup tags for titles, prices, and marks.
        """
        soup = BeautifulSoup(response.content, "html.parser")
        titles = soup.find_all("span", class_="product_name")
        prices = soup.find_all("span", class_="price ak1")
        marks = soup.find_all("p", class_="product-desc-desktop")
        return titles, prices, marks


if __name__ == "__main__":
    scraper = AviandcoScraper()
    base_url = "https://www.aviandco.com/catalogsearch/result/index/?p={}&q=rolex"
    scraper.scrape_all_pages(base_url)
    scraper.save_to_csv(filename="aviandco.csv", include_mark=True)
