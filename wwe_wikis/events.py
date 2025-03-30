import duckdb
import pandas as pd
import wikipedia as wp
from loguru import logger
from icecream import ic
import mechanicalsoup


def events(db_path: str, table: str) -> None:
    """
    process the wwe wiki event tables
    """

    wiki_pages = [
        "WrestleMania"
    ]

    for i, page in enumerate(wiki_pages, 1):
        logger.info(f"Processing {page} ({i} of {len(wiki_pages)})")
        html = wp.page("SummerSlam").html()
        
        with open('wrestlemania.html', 'w', encoding='utf-8') as f:
            f.write(html)

        ic(html)

        try:
            # Attempt to parse the table using pandas
            df = pd.read_html(html, match="Event")[0]
            logger.info(f"Dataframe for {page} created using pandas")
        except ValueError as e:
            logger.warning(f"Pandas failed to parse HTML table for {page}: {e}")
            # Fallback to MechanicalSoup
            browser = mechanicalsoup.Browser()
            soup = mechanicalsoup.StatefulBrowser().page_soup(html)
            table = soup.find("table", {"class": "wikitable"})
            rows = table.find_all("tr")
            data = [[cell.get_text(strip=True) for cell in row.find_all(["th", "td"])] for row in rows]
            df = pd.DataFrame(data[1:], columns=data[0])
            logger.info(f"Dataframe for {page} created using MechanicalSoup")

        print(df)

        logger.info(f"Loading {page} into duckdb")
        with duckdb.connect(database=db_path) as con:
            con.execute("INSERT INTO champs SELECT * FROM df")
        logger.success(f"Data from {page} loaded successfully")
