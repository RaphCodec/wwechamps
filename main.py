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
    diagnose=True,
    colorize=True,
)


def main():
    html = wp.page("List_of_WWE_Champions").html()
    wwe_champs = pd.read_html(html)[2]
    logger.info("Dataframe wwe_champs created")

    wwe_champs.columns = wwe_champs.columns.droplevel(0).str.lower()
    wwe_champs = wwe_champs[
        pd.to_numeric(wwe_champs["no."], errors="coerce").notna()
    ].drop(wwe_champs.columns[-2:], axis=1)

    champions = pd.DataFrame(data={"champion": wwe_champs["champion"].unique()})
    champions.insert(loc=0, column="championID", value=champions.index + 1)

    events = pd.DataFrame(data={"event": wwe_champs["event"].unique()})
    events.insert(loc=0, column="eventID", value=events.index + 1)

    locations = pd.DataFrame(data={"location": wwe_champs["location"].unique()})
    locations.insert(loc=0, column="locationID", value=locations.index + 1)

    wwe_champs["champion"] = wwe_champs["champion"].map(
        champions.set_index("champion")["championID"].to_dict()
    )
    wwe_champs["event"] = wwe_champs["event"].map(
        events.set_index("event")["eventID"].to_dict()
    )
    wwe_champs["location"] = wwe_champs["location"].map(
        locations.set_index("location")["locationID"].to_dict()
    )

    wwe_champs = wwe_champs.assign(
        **{
            "no.": pd.to_numeric(wwe_champs["no."], errors="coerce"),
            "days": pd.to_numeric(wwe_champs["days"], errors="coerce"),
            "reign": pd.to_numeric(wwe_champs["reign"], errors="coerce"),
            "days recog.": pd.to_numeric(wwe_champs["days recog."], errors="coerce"),
            "date": pd.to_datetime(
                wwe_champs["date"], format="%B %d, %Y", errors="coerce"
            ),
            "title": "WWE Championship",
        }
    ).rename(columns={"no.": "no", "days recog.": "days_recognized"})

    logger.info("Loading data into duckdb")

    con = duckdb.connect(database="data/wwe.duckdb")
    con.execute("CREATE OR REPLACE TABLE champions AS SELECT * FROM champions")
    con.execute("CREATE OR REPLACE TABLE events AS SELECT * FROM events")
    con.execute("CREATE OR REPLACE TABLE locations AS SELECT * FROM locations")
    con.execute("CREATE OR REPLACE TABLE wwe_champs AS SELECT * FROM wwe_champs")
    con.execute("ALTER TABLE wwe_champs ADD PRIMARY KEY (no)")
    con.execute("ALTER TABLE champions ADD PRIMARY KEY (championID)")
    con.execute("ALTER TABLE events ADD PRIMARY KEY (eventID)")
    con.close()
    logger.success("database created successfully")

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
