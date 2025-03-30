from datetime import datetime

import duckdb
import pandas as pd
import wikipedia as wp
from loguru import logger

logger.add(
    "logs/{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="7 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    backtrace=True,
)


def process_title_changes(df: pd.DataFrame, title: str) -> int:
    """
    Process the Wiki tables containing title changes
    """
    df = (
        df[pd.to_numeric(df["no."], errors="coerce").notna()]
        .loc[:, :"notes"]
        .rename(columns={"no.": "title_reign", "days recog.": "days_recognized"})
    )

    df = df.assign(
        reign=pd.to_numeric(df["reign"], errors="coerce"),
        date=pd.to_datetime(df["date"], format="%B %d, %Y", errors="coerce").dt.date,
        days=pd.to_numeric(df["days"].str.replace("<1", "0"), errors="coerce"),
        days_recognized=pd.to_numeric(
            df["days_recognized"].str.replace("<1", "0"), errors="coerce"
        ),
        title=title,
    )

    logger.info(f"Loading {title} into duckdb")
    with duckdb.connect(database="data/wwe.duckdb") as con:
        con.execute("INSERT INTO champs SELECT * FROM df")
    logger.success(f"Data from {title} loaded successfully")

    return len(df)


def main():
    with duckdb.connect(database="data/wwe.duckdb") as con:
        with open("sql/table.sql", "r") as f:
            sql = f.read()
        con.execute(sql)
        logger.success("Table created successfully")
    logger.info("Loading data from Wikipedia")

    wiki_pages = [
        ("List_of_WWE_Champions", "WWE Championship"),
        (
            "List_of_World_Heavyweight_Champions_(WWE,_2002–2013)",
            "World Heavyweight Championship",
        ),
        (
            "World_Heavyweight_Championship_(WWE)",
            "World Heavyweight Championship (WWE)",
        ),
        ("List_of_WWE_Intercontinental_Champions", "Intercontinental Championship"),
        ("List_of_WWE_United_States_Champions", "United States Championship"),
        ("List of WWE Women's Champions (1956–2010)", "WWE Women's Championship"),
        ("List of Women's World Champions (WWE)", "Women's World Championship"),
        ("List of WWE Women's Champions", "WWE Women's Championship (2016)"),
        ("List of WWE Divas Champions", "WWE Divas Championship"),
        (
            "List of ECW World Heavyweight Champions",
            "ECW World Heavyweight Championship",
        ),
        ("List of World Tag Team Champions (WWE)", "World Tag Team Championship"),
        (
            "List of World Tag Team Champions (WWE, 1971–2010)",
            "World Tag Team Championship (WWE)",
        ),
        ("List of WWE Tag Team Champions", "WWE Tag Team Championship"),
        ("List of WWE Women's Tag Team Champions", "WWE Women's Tag Team Championship"),
    ]

    for i, (page, title) in enumerate(wiki_pages, 1):
        logger.info(f"Processing {title} ({i} of {len(wiki_pages)})")
        html = wp.page(page).html()

        if i in [3]:
            title_changes = pd.read_html(html)[3]
        elif i in [14]:
            title_changes = pd.read_html(html)[1]
        else:
            title_changes = pd.read_html(html)[2]
        logger.info(f"Dataframe for {title} created")

        title_changes.columns = title_changes.columns.droplevel(0).str.lower()

        if i in [12]:
            title_changes.insert(
                title_changes.columns.get_loc("days") + 1, "days recog.", None
            )

        rows = process_title_changes(df=title_changes, title=title)
        logger.info(f"Rows inserted: {rows:,}")

    return


if __name__ == "__main__":
    start = datetime.now()
    logger.info("Script Started")

    try:
        main()
        elapsed = datetime.now() - start
        logger.success(
            f"Script Ran Sucessfully. {elapsed.seconds // 3600} hours {elapsed.seconds % 3600 // 60} minutes {elapsed.seconds % 60} seconds elapsed"
        )
    except Exception as e:
        elapsed = datetime.now() - start
        logger.error(
            f"Script Failed. {elapsed.seconds // 3600} hours {elapsed.seconds % 3600 // 60} minutes {elapsed.seconds % 60} seconds elapsed"
        )

        logger.exception(f"Error: {e}")
