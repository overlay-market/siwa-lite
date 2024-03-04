from typing import List
from bs4 import BeautifulSoup
from base_scraper import BaseScraper


class Chrono24Scraper(BaseScraper):
    """
    A web scraper for extracting watch names and prices from Chrono24.

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
        titles = soup.find_all(
            "div", class_="text-sm text-sm-md text-bold text-ellipsis"
        )
        prices = soup.find_all("span", class_="currency")
        marks = soup.find_all("div", class_="text-sm text-sm-md text-ellipsis m-b-2")
        return titles, prices, marks


if __name__ == "__main__":
    # Main function to initiate scraping and saving
    scraper = Chrono24Scraper()
    base_url = "https://www.chrono24.com/rolex/index-{}.htm?query=Rolex"
    scraper.scrape_all_pages(base_url)
    scraper.save_to_csv(filename="chrono24.csv", include_mark=True)
