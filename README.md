# Montréal Centris Rental Sync

Small Python utility that pulls the **latest rental listings** in Montréal from
an Apify Actor (`aitorsm/centris-scraper`) and inserts them into our
PostgreSQL / PostGIS schema.

<p align="center">
  <img src="https://raw.githubusercontent.com/your-org/appart-ai/main/docs/centris-sync-flow.svg" width="650">
</p>

---

## 1 – Prerequisites

| Tool              | Version         | Notes                        |
| ----------------- | --------------- | ---------------------------- |
| **Python**        | 3.9 +           | Tested on 3.11               |
| **PostgreSQL**    | 13 +            | With the `postgis` extension |
| **Apify account** | Free tier works | Needs an API token           |

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 2 – Environment variables

Create a .env file next to centris_sync.py:

## 3 – Run manually

```bash
python centris_sync.py
```
