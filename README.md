# Data

## Dataset

| | |
|---|---|
| **Name** | Novel Corona Virus 2019 Dataset (file: `covid_19_data.csv`) |
| **Publisher** | Kaggle, by Sudalairaj Kumar (`sudalairajkumar`) |
| **Original source** | Johns Hopkins University CSSE COVID-19 data |
| **Kaggle page** | https://www.kaggle.com/datasets/sudalairajkumar/novel-corona-virus-2019-dataset |
| **Coverage** | 2020-01-22 to 2021-05-29 (the dataset is no longer updated) |
| **Size** | 306,429 rows, about 22 MB |
| **Format** | CSV, one row per province/state per country per day |

The file provides cumulative `Confirmed`, `Deaths` and `Recovered` values per day.
The producer derives the daily `new_cases` / `new_deaths` from those totals.

## Download

The CSV is **not committed to git** (22 MB). Download it once and put it here as
`data/covid_19_data.csv`.

**Option A: browser.** Open the Kaggle page above, sign in, click *Download*, unzip, and
copy `covid_19_data.csv` into this folder.

**Option B: Kaggle CLI.** Requires a Kaggle account and an API token
(`~/.kaggle/kaggle.json`):

```bash
pip install kaggle
kaggle datasets download -d sudalairajkumar/novel-corona-virus-2019-dataset -f covid_19_data.csv -p data/
unzip data/covid_19_data.csv.zip -d data/
```

Check the download:

```bash
head -3 data/covid_19_data.csv
```

Expected header:

```
SNo,ObservationDate,Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered
```

## Columns

| Column | Type | Meaning | Used by producer |
|---|---|---|---|
| `SNo` | int | Row number | no |
| `ObservationDate` | `MM/DD/YYYY` | Day of the observation | yes -> `date` |
| `Province/State` | text | Region within a country (empty for about 25% of rows) | yes, summed per country |
| `Country/Region` | text | Country | yes -> `country` |
| `Last Update` | text | Source timestamp | no |
| `Confirmed` | float | **Cumulative** confirmed cases | yes -> `total_cases` |
| `Deaths` | float | **Cumulative** deaths | yes -> `total_deaths` |
| `Recovered` | float | **Cumulative** recoveries (0 often means "not reported") | yes -> `total_recovered` |

## Known data quirks (handled in `producer/producer.py`)

- Values are cumulative, so `new_cases` and `new_deaths` are computed as today's total
  minus yesterday's. They are `null` for a country's first day, after a gap in dates, or
  when the total went down (a source correction).
- 5 duplicate (date, province, country) rows; the row with the largest `Confirmed` is kept.
- 1 negative `Confirmed` value and 2 negative `Deaths` values; affected days are sent as
  `null` rather than a wrong number.
- `Recovered = 0` frequently means the country does not report recoveries, so it is sent
  as `null`, not `0`.
- A few country names have stray formatting, such as a leading space or the literal
  `('St. Martin',)`. Whitespace is trimmed; renaming and standardising countries is left to
  the NiFi step.
- The producer sums provinces into one row per country per day (about 87,000 messages
  across 228 countries).

## Using it with the producer

Run from the **repository root** (the default CSV path is relative):

```bash
docker compose up -d
pip install -r producer/requirements.txt
python producer/producer.py
python producer/producer.py data/covid_19_data.csv 2021-05-01 2021-05-29
```

Each message is JSON, keyed by country, sent to the `covid-data` topic:

```json
{"country": "Afghanistan", "date": "2021-05-28", "new_cases": 764, "new_deaths": 12,
 "total_cases": 69130, "total_deaths": 2881, "total_recovered": 57119, "active_cases": 9130}
```