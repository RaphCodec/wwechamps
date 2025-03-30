import os
from datetime import datetime

import duckdb
from loguru import logger

from wwe_wikis import titles, events

logger.add(
    "logs/{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="7 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    backtrace=True,
)


def main() -> None:
    db_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "data/wwe.duckdb")
    )

    # with duckdb.connect(database=db_path) as con:
    #     with open("sql/table.sql", "r") as f:
    #         sql = f.read()
    #     con.execute(sql)
    #     logger.success("Table created successfully")
    # logger.info("Loading titles data from Wikipedia")

    # titles(db_path, table="champs")
    events(db_path, table="events")

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
