CREATE OR REPLACE TABLE champs(
  title_reign INTEGER,
  champion VARCHAR,
  date DATE,
  "event" VARCHAR,
  "location" VARCHAR,
  reign INTEGER,
  "days" INTEGER,
  days_recognized INTEGER,
  notes VARCHAR,
  title VARCHAR,
  PRIMARY KEY(title_reign, champion, title)
);