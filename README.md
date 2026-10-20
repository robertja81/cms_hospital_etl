# CMS Hospitals ETL

Downloads every dataset tagged with the "Hospitals" theme from the [CMS Provider Data metastore](https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items), converts the column headers to snake_case, and saves the CSVs locally. It's built to run once a day and only downloads datasets that changed since the last run.

## Running it

You only need Python 3.9 or newer. There are no packages to install, since everything used is in the standard library. That's why `requirements.txt` is empty.

```
python cms_hospitals_etl.py                  # normal daily run
python cms_hospitals_etl.py --full-refresh   # ignore history, download everything
```

Other options are `--workers` (default 8), `--output-dir` (default `./output`) and `--state-db` (default `./cms_etl_state.db`).

To run it daily, use cron on Linux:

```
0 6 * * * cd /opt/cms_hospital_etl && python3 cms_hospitals_etl.py >> etl.log 2>&1
```

or Task Scheduler on Windows:

```
schtasks /create /tn "CMS Hospitals ETL" /sc daily /st 06:00 /tr "python C:\path\to\cms_hospitals_etl.py"
```

## How it works

1. Pulls the full metastore list (237 datasets) and keeps the ones whose `theme` includes "Hospitals" (75 right now).
2. Compares each dataset's `modified` date to the one saved from its last successful load in SQLite. Only new or changed datasets get downloaded.
3. Downloads and processes the changed files in parallel on a thread pool. Each file is streamed row by row, so memory use stays flat even on the 325k-row HCAHPS file.
4. Rewrites the header row in snake_case. For example, `Patients' rating of the facility linear mean score` becomes `patients_rating_of_the_facility_linear_mean_score`, and `MedCont %` becomes `med_cont`.
5. Records the result in SQLite. There's one row per run in `runs`, and one row per dataset in `datasets`.

Some things I did on purpose:

- **A failed download isn't recorded as loaded**, so the next run picks it up again automatically. One bad file also doesn't stop the other 74.
- **Files are written to a `.tmp` file and renamed when complete**, so a crash can't leave a half-written CSV in the output folder.
- **Requests retry with backoff.** The CMS API does time out sometimes. Run 1 in `sample_output/state_runs.csv` is a real example: the metastore call timed out three times, the run was marked `failed`, and the next run picked everything up.
- **It exits with code 1 if anything failed**, so cron, Task Scheduler or Airflow can alert on it.

## Sample output

The `sample_output/` folder contains:

- the first 10 rows of three processed files
- `header_mapping.csv`, which lists every original header and what it became (1,296 headers across 75 datasets)
- `state_runs.csv` and `state_datasets.csv`, which are dumps of the tracking tables

## What I'd change for production

- Store state in a shared database such as Postgres or Azure SQL instead of a local SQLite file, and write output to blob storage.
- Check the HTTP `ETag`/`Last-Modified` header as well. The metastore `modified` field is only a date, so two updates on the same day would look like one.
- Write Parquet instead of CSV, and keep dated snapshots instead of overwriting files.
- Add basic data quality checks, such as expected columns and big row-count swings, before marking a dataset as loaded.
