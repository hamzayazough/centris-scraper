import os
import re
import uuid
import time
import datetime as dt
from dateutil import parser as dtparser
from dotenv import load_dotenv
from apify_client import ApifyClient
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()
START = time.time()

client = ApifyClient(os.environ["APIFY_API_TOKEN"])
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor(cursor_factory=RealDictCursor)

print("🔑 Connexions…")
print("✅ OK\n")

print("🚀 Scraping Centris (maxItems = 2)…")
run = client.actor("aitorsm/centris-scraper").call(run_input={
    "startUrls": [{"url": "https://www.centris.ca/en/properties~for-rent~montreal"}],
    "maxItems": 2,
    "sort": "date_desc",
    "proxy": {"useApifyProxy": True},
})
dataset = client.dataset(run["defaultDatasetId"])
print("✅ Actor terminé – dataset chargé\n")

LISTING_KEYS = ["offer_date", "date_listed", "listing_date", "posted_on"]
MOVEIN_KEYS  = ["move-in-date", "available_from"]

PRICE_RE = re.compile(r"\$?\s*([0-9]{3,5})\s*(?:\$|cad|/mo|per month)?", re.I)
SQFT_RE  = re.compile(r"([0-9]{3,5})\s*(?:sq ?ft|pi2|pieds)", re.I)

PETS_PAT  = re.compile(r"(pets?|animaux).{0,20}(allowed|permis|accept)", re.I)
SMOKE_PAT = re.compile(r"(non[- ]?)?smoking|fumeur|tabac", re.I)

def first_date(prop, keys):
    for k in keys:
        raw = prop.get(k)
        if raw:
            try:
                return dtparser.parse(str(raw), fuzzy=True).date()
            except Exception:
                pass
    return None

def price_from_text(txt):
    m = PRICE_RE.search(txt or "")
    return float(m.group(1)) if m else None

def sqft_from_text(txt):
    m = SQFT_RE.search(txt or "")
    return int(m.group(1)) if m else None

inserted = skipped = 0

for prop in dataset.iterate_items():
    url   = prop["url"]
    place = prop["address"]

    cur.execute("SELECT 1 FROM accommodations WHERE details_link=%s LIMIT 1", (url,))
    if cur.fetchone():
        skipped += 1
        continue

    cur.execute("SELECT id FROM addresses WHERE place_name=%s LIMIT 1", (place,))
    row = cur.fetchone()
    if row:
        addr_id = row["id"]
    else:
        lat, lon = prop["coordinates"]["latitude"], prop["coordinates"]["longitude"]
        wkt      = f"SRID=4326;POINT({lon} {lat})"
        addr_id  = uuid.uuid4()
        cur.execute(
            "INSERT INTO addresses (id, place_name, location) VALUES (%s,%s,%s)",
            (str(addr_id), place, wkt)
        )

    desc_raw = prop.get("description") or ""
    txt_feat = (prop.get("additional_features") or "") + " " + desc_raw
    txt_low  = txt_feat.lower()

    rent_price      = prop.get("price") or price_from_text(desc_raw) or 0.0
    sqft            = prop.get("living_sqft") or prop.get("net_sqft") or sqft_from_text(desc_raw)
    num_beds        = prop.get("beds_total")
    num_baths       = prop.get("baths_total")
    year_built      = prop.get("year_built")
    has_pool        = bool(prop.get("pool"))
    gym_bool        = "gym" in txt_low or "fitness" in txt_low
    parking_inc     = bool(prop.get("parking_total") or prop.get("parking_garage"))
    pets_allowed    = bool(PETS_PAT.search(txt_feat))
    smoking_allowed = bool(SMOKE_PAT.search(txt_feat) and "non smoking" not in txt_low)

    title = prop.get("category")
    if not desc_raw:
        desc_raw = (
            f"{num_beds or '?'}-bed "
            f"{title.lower() if title else 'unit'} at {place}. "
            f"Rent {'N/A' if rent_price == 0 else rent_price} CAD."
        )

    offer_date     = first_date(prop, LISTING_KEYS) or dt.date.today()
    available_from = first_date(prop, MOVEIN_KEYS)  or dt.date.today()

    phone = None
    brokers = prop.get("listing_brokers") or []
    if brokers and brokers[0].get("phone_numbers"):
        phone = brokers[0]["phone_numbers"][0]

    acc_id = uuid.uuid4()
    cur.execute(
        """
        INSERT INTO accommodations (
            id, title, description, rent_price, num_beds, num_bathrooms, square_footage,
            construction_date, has_pool, gym, parking_included,
            lease_duration, roommate_accepted, pets_allowed, smoking_allowed,
            owner_cellphone,
            address_id, details_link, available_from, offer_date
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            str(acc_id), title, desc_raw, rent_price, num_beds, num_baths, sqft,
            year_built, has_pool, gym_bool, parking_inc,
            12, True, pets_allowed, smoking_allowed,
            phone,
            str(addr_id), url, available_from, offer_date,
        )
    )

    for photo in prop.get("photos") or []:
        href = photo["href"]
        cur.execute(
            "INSERT INTO image_urls (image_url) VALUES (%s) ON CONFLICT DO NOTHING",
            (href,)
        )
        cur.execute(
            """
            INSERT INTO accommodation_images (
                accommodation_id, image_url
            ) VALUES (%s,%s) ON CONFLICT DO NOTHING
            """,
            (str(acc_id), href)
        )

    inserted += 1
    print(f"✅  {place[:40]}…  rent={rent_price}  pets={pets_allowed} smoke={smoking_allowed}")

conn.commit()
cur.close()
conn.close()

print(f"\n🎉  {inserted} ajout(s), {skipped} doublon(s) – {time.time()-START:.1f}s")
