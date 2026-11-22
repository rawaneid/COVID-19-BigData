"""Member 1 - Data source & Python producer (data preparation part)."""

import json
import sys
import time

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC = "covid-data"
SERVERS = "localhost:9092"
DELAY_SECONDS = 1  # pause between two days, to simulate real-time streaming


def load_raw(csv_path):
    """Read only the columns we need from the Kaggle CSV."""
    return pd.read_csv(
        csv_path,
        usecols=["ObservationDate", "Province/State", "Country/Region", "Confirmed", "Deaths", "Recovered"],
    )


def prepare_rows(raw):
    """Basic formatting only: parse dates, trim text, convert numbers, drop duplicate rows."""
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(raw["ObservationDate"], format="%m/%d/%Y", errors="coerce"),
            # trimming spaces is formatting; renaming countries is NiFi's job (Member 3)
            "country": raw["Country/Region"].astype("string").str.strip(),
            "province": raw["Province/State"].fillna("").astype(str).str.strip(),
            "confirmed": pd.to_numeric(raw["Confirmed"], errors="coerce"),
            "deaths": pd.to_numeric(raw["Deaths"], errors="coerce"),
            "recovered": pd.to_numeric(raw["Recovered"], errors="coerce"),
        }
    )
    before = len(df)
    df = df.dropna(subset=["date", "country"])
    df = df[df["country"] != ""]
    print(f"dropped {before - len(df)} rows with invalid date or missing country")

    # duplicates: same day + province + country -> keep the row with the largest Confirmed
    df = df.sort_values("confirmed", na_position="first").drop_duplicates(
        ["date", "province", "country"], keep="last"
    )

    # invalid values (negative or missing) are flagged, never silently turned into 0
    df["bad_cases"] = df["confirmed"].isna() | (df["confirmed"] < 0)
    df["bad_deaths"] = df["deaths"].isna() | (df["deaths"] < 0)
    df["bad_recovered"] = df["recovered"].isna() | (df["recovered"] < 0)
    return df


def aggregate_daily(rows):
    """One row per (country, date): sum the provinces/states of that country."""
    daily = (
        rows.groupby(["country", "date"], as_index=False)
        .agg(
            total_cases=("confirmed", "sum"),
            total_deaths=("deaths", "sum"),
            total_recovered=("recovered", "sum"),
            bad_cases=("bad_cases", "any"),
            bad_deaths=("bad_deaths", "any"),
            bad_recovered=("bad_recovered", "any"),
        )
        .sort_values(["country", "date"])
        .reset_index(drop=True)
    )
    # a day whose sum contains an invalid row is not trustworthy -> null, not a wrong number
    daily.loc[daily["bad_cases"], "total_cases"] = float("nan")
    daily.loc[daily["bad_deaths"], "total_deaths"] = float("nan")
    daily.loc[daily["bad_recovered"], "total_recovered"] = float("nan")
    return daily


def mask_unreported_recovered(daily):
    """The source writes Recovered = 0 when a country does NOT report recoveries.
    That 0 means "unknown", so it becomes null. A 0 is kept only in the early days
    of a country that starts reporting recoveries later (a real 0)."""
    rec = daily["total_recovered"]
    by_country = rec.fillna(0).groupby(daily["country"])
    ever_reported = by_country.transform("max") > 0   # country has a positive Recovered at some point
    reported_before = by_country.cummax() > 0         # ... on or before this day
    unreported = (rec == 0) & (daily["total_cases"] > 0) & (~ever_reported | reported_before)
    daily.loc[unreported, "total_recovered"] = float("nan")
    return daily


def add_new_values(daily):
    """new_cases / new_deaths = today's total minus yesterday's total (null when unknown)."""
    by_country = daily.groupby("country")
    # yesterday must really be the previous calendar day (no gap in dates)
    consecutive = (daily["date"] - by_country["date"].shift(1)) == pd.Timedelta(days=1)
    for total, new in (("total_cases", "new_cases"), ("total_deaths", "new_deaths")):
        diff = by_country[total].diff()
        # no previous day, a gap, or a negative difference (source correction) -> null
        daily[new] = diff.where(consecutive & (diff >= 0))
    return daily


def partial_recovered_mask(daily, min_cases=10_000, min_ratio=0.2):
    """True for days where Recovered is only partially reported, so active cases would be wrong.
    Rule: from the day a country (with >= min_cases) has recovered/confirmed below min_ratio
    and stays below it until the end of the data. A low ratio that later recovers is just
    a normal early wave, so it is NOT flagged."""
    ratio = (daily["total_recovered"] / daily["total_cases"]).astype("float64")
    future_max = ratio[::-1].groupby(daily["country"][::-1]).cummax()[::-1]  # best ratio from this day on
    return (future_max < min_ratio) & (daily["total_cases"] >= min_cases)


def clean(raw):
    """Raw Kaggle rows -> one row per (country, date) with totals, new_* and active cases."""
    daily = aggregate_daily(prepare_rows(raw))
    daily = mask_unreported_recovered(daily)
    daily = add_new_values(daily)

    # active = confirmed - deaths - recovered; null if any part is unknown, the result is negative,
    # or the country only reports a small part of its recoveries (see partial_recovered_mask)
    active = daily["total_cases"] - daily["total_deaths"] - daily["total_recovered"]
    daily["active_cases"] = active.where((active >= 0) & ~partial_recovered_mask(daily))

    numbers = ["total_cases", "total_deaths", "total_recovered", "new_cases", "new_deaths", "active_cases"]
    for col in numbers:
        daily[col] = daily[col].astype("Int64")  # whole numbers, missing = <NA> (not 0)
    return daily[
        ["country", "date", "new_cases", "new_deaths", "total_cases", "total_deaths",
         "total_recovered", "active_cases"]
    ]


def _to_int(value):
    """pandas number -> plain Python int, or None (JSON null) when missing."""
    return None if pd.isna(value) else int(value)


def to_message(row):
    """One cleaned row -> the dictionary that becomes the JSON message."""
    return {
        "country": row.country,
        "date": row.date.strftime("%Y-%m-%d"),
        "new_cases": _to_int(row.new_cases),
        "new_deaths": _to_int(row.new_deaths),
        "total_cases": _to_int(row.total_cases),
        "total_deaths": _to_int(row.total_deaths),
        "total_recovered": _to_int(row.total_recovered),
        "active_cases": _to_int(row.active_cases),
    }


def iter_daily_batches(df, start_date=None, end_date=None):
    """Yield (date_string, [messages]) in date order; all countries of one day together."""
    if start_date:
        df = df[df["date"] >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df["date"] <= pd.Timestamp(end_date)]
    for date, day in df.sort_values(["date", "country"]).groupby("date"):
        yield date.strftime("%Y-%m-%d"), [to_message(r) for r in day.itertuples(index=False)]


def create_producer():
    """Connect to the Kafka broker; stop with a clear message if it is not reachable."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=SERVERS,
            acks="all",  # wait until Kafka confirms every message
        )
        print(f"Connected to Kafka at {SERVERS}")
        return producer
    except KafkaError as exc:  # raised when the broker is not reachable
        print(f"ERROR: cannot connect to Kafka at {SERVERS}. Is the Docker container running? ({exc})")
        sys.exit(1)


def serialize(message):
    """Message dict -> (key, value) bytes. Key = country, so one country always uses the same partition."""
    return message["country"].encode("utf-8"), json.dumps(message).encode("utf-8")


def on_send_error(exc):
    """Called by Kafka when one message could not be delivered."""
    print(f"  ERROR: message failed: {exc}")


def main():
    # usage: python producer.py <csv_path> [start_date] [end_date]
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/covid_19_data.csv"
    start_date = sys.argv[2] if len(sys.argv) > 2 else None
    end_date = sys.argv[3] if len(sys.argv) > 3 else None

    try:
        df = clean(load_raw(csv_path))
    except FileNotFoundError:
        print(f"ERROR: CSV file not found: {csv_path}. Run from the repo root or pass the path (see data/README.md).")
        sys.exit(1)
    except (ValueError, KeyError) as exc:
        print(f"ERROR: could not read {csv_path} as the Kaggle covid_19_data.csv file ({exc})")
        sys.exit(1)
    producer = create_producer()
    total = 0
    try:
        for date, messages in iter_daily_batches(df, start_date, end_date):
            try:
                for m in messages:
                    key, value = serialize(m)
                    producer.send(TOPIC, key=key, value=value).add_errback(on_send_error)
                producer.flush()  # wait until the whole day is delivered
                total += len(messages)
                print(f"{date}: sent {len(messages)} records to '{TOPIC}'")
            except KafkaError as exc:
                print(f"{date}: ERROR while sending: {exc}")
            time.sleep(DELAY_SECONDS)
    except KeyboardInterrupt:
        print("Stopped by the user")
    finally:
        producer.close()
    print(f"Done. Total records sent: {total}")


if __name__ == "__main__":
    main()