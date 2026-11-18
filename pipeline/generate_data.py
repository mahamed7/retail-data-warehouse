"""Generate the synthetic nightly extracts of Dukaan Retail's point-of-sale system.

Dukaan Retail is a fictional chain of eight shops in Somaliland. This script writes
the five CSV files described in ``docs/data-contract.md`` (stores, products,
customers, orders, order_items) to ``data/raw/``.

The output is fully deterministic: every random draw comes from a single
``random.Random(42)`` instance, so two runs produce byte-identical files.

Besides realistic patterns (bigger stores sell more, Ramadan / Eid and December
peaks, a busy Thursday-Friday weekend, a mid-2025 price increase), the extracts
contain the data quality issues the contract promises, so the dbt staging layer
has real cleaning work to do:

* customers: ~2% exact duplicate rows, inconsistent name casing, emails with
  stray whitespace and upper case, ~3% empty emails;
* orders: ~1% of order ids re-sent after a status change (latest ``updated_at``
  wins), ~30% walk-in orders with an empty ``customer_id``;
* order_items: ~0.1% of lines reference a product missing from products.csv,
  ``discount_amount`` left empty when there is no discount.

Usage::

    python pipeline/generate_data.py [--out data/raw]
"""

from __future__ import annotations

import argparse
import csv
import random
from bisect import bisect_right
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from itertools import accumulate
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")

# --------------------------------------------------------------------------- #
# Run configuration
# --------------------------------------------------------------------------- #
SEED = 42
PERIOD_START = date(2024, 1, 1)
PERIOD_END = date(2025, 12, 31)
DEFAULT_OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

N_CUSTOMERS = 2_000
N_ORDERS = 25_000
FIRST_CUSTOMER_ID = 5001
FIRST_ORDER_ID = 100001
FIRST_PRODUCT_ID = 101

# --------------------------------------------------------------------------- #
# File layouts (column order is part of the contract)
# --------------------------------------------------------------------------- #
STORE_COLUMNS = ("store_id", "store_name", "city", "region", "store_type", "opened_date")
PRODUCT_COLUMNS = (
    "product_id", "sku", "product_name", "category", "subcategory",
    "unit_cost", "unit_price", "is_active",
)
CUSTOMER_COLUMNS = (
    "customer_id", "first_name", "last_name", "email", "phone", "city", "signup_date",
)
ORDER_COLUMNS = (
    "order_id", "customer_id", "store_id", "order_ts", "channel",
    "payment_method", "status", "updated_at",
)
ORDER_ITEM_COLUMNS = (
    "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount",
)

# --------------------------------------------------------------------------- #
# Injected data quality issues
# --------------------------------------------------------------------------- #
CUSTOMER_DUPLICATE_RATE = 0.02
EMPTY_EMAIL_RATE = 0.03
EMAIL_UPPER_RATE = 0.05
EMAIL_TITLE_CASE_RATE = 0.20
EMAIL_WHITESPACE_RATE = 0.15
NAME_UPPER_RATE = 0.08
NAME_LOWER_RATE = 0.08
RESENT_ORDER_RATE = 0.01
WALK_IN_RATE = 0.30
ORPHAN_ITEM_RATE = 0.001
ORPHAN_PRODUCT_IDS = (233, 241, 256, 278)  # products deleted from the master data

# --------------------------------------------------------------------------- #
# Stores: (id, name, city, region, type, opened, relative traffic)
# --------------------------------------------------------------------------- #
STORES = (
    (1, "Dukaan Hargeisa Central", "Hargeisa", "Maroodi Jeex", "Supermarket", date(2017, 3, 15), 3.0),
    (2, "Dukaan Borama Market", "Borama", "Awdal", "Supermarket", date(2018, 9, 1), 1.8),
    (3, "Dukaan Berbera Port", "Berbera", "Saaxil", "Wholesale", date(2019, 6, 1), 1.6),
    (4, "Dukaan Burao Main Street", "Burao", "Togdheer", "Supermarket", date(2018, 2, 10), 2.0),
    (5, "Dukaan Gabiley", "Gabiley", "Maroodi Jeex", "Mini-mart", date(2020, 11, 20), 0.8),
    (6, "Dukaan Erigavo", "Erigavo", "Sanaag", "Mini-mart", date(2021, 5, 5), 0.7),
    (7, "Dukaan Sheikh", "Sheikh", "Togdheer", "Mini-mart", date(2022, 1, 15), 0.5),
    (8, "Dukaan Wajaale Border", "Wajaale", "Maroodi Jeex", "Wholesale", date(2021, 8, 30), 1.0),
)

# --------------------------------------------------------------------------- #
# Product catalogue: category -> (SKU prefix, popularity, [(name, subcategory, list price)])
# --------------------------------------------------------------------------- #
CATALOGUE: dict[str, tuple[str, float, list[tuple[str, str, float]]]] = {
    "Groceries": ("GRC", 1.4, [
        ("Basmati Rice 5kg", "Rice & Grains", 6.25),
        ("Basmati Rice 10kg", "Rice & Grains", 11.90),
        ("Sorghum 2kg", "Rice & Grains", 2.30),
        ("Spaghetti 500g", "Pasta", 0.85),
        ("Macaroni 500g", "Pasta", 0.80),
        ("Wheat Flour 2kg", "Flour & Baking", 1.95),
        ("Wheat Flour 25kg", "Flour & Baking", 19.50),
        ("Sugar 1kg", "Sugar", 1.10),
        ("Sugar 50kg", "Sugar", 42.00),
        ("Vegetable Oil 1L", "Cooking Oil", 2.20),
        ("Vegetable Oil 5L", "Cooking Oil", 9.75),
        ("Sesame Oil 1L", "Cooking Oil", 3.40),
        ("Red Lentils 1kg", "Pulses & Beans", 1.45),
        ("Kidney Beans 1kg", "Pulses & Beans", 1.60),
        ("Powdered Milk 900g", "Dairy", 6.80),
        ("Canned Tuna 185g", "Canned Food", 1.25),
        ("Tomato Paste 400g", "Canned Food", 0.95),
        ("Dates 1kg", "Dried Fruit", 3.50),
        ("Honey 500g", "Spreads", 5.75),
        ("Xawaash Spice Mix 250g", "Spices", 2.10),
    ]),
    "Beverages": ("BEV", 1.2, [
        ("Bottled Water 500ml", "Water", 0.30),
        ("Bottled Water 1.5L", "Water", 0.55),
        ("Bottled Water 6x1.5L", "Water", 2.90),
        ("Cola 330ml Can", "Soft Drinks", 0.60),
        ("Cola 1.5L", "Soft Drinks", 1.35),
        ("Orange Soda 330ml Can", "Soft Drinks", 0.60),
        ("Malt Drink 330ml", "Soft Drinks", 0.75),
        ("Mango Juice 1L", "Juice", 1.80),
        ("Orange Juice 1L", "Juice", 1.85),
        ("Guava Nectar 250ml", "Juice", 0.55),
        ("Black Tea Leaves 500g", "Tea & Coffee", 2.40),
        ("Green Tea 25 Bags", "Tea & Coffee", 1.70),
        ("Instant Coffee 200g", "Tea & Coffee", 5.20),
        ("Ground Coffee 250g", "Tea & Coffee", 4.60),
        ("Cardamom Tea Mix 250g", "Tea & Coffee", 2.75),
        ("Energy Drink 250ml", "Energy Drinks", 1.10),
        ("Long-Life Milk 1L", "Milk Drinks", 1.40),
        ("Camel Milk 1L", "Milk Drinks", 1.60),
        ("Yoghurt Drink 500ml", "Milk Drinks", 0.90),
        ("Sparkling Water 330ml", "Water", 0.65),
    ]),
    "Household": ("HSH", 0.8, [
        ("Laundry Detergent 1kg", "Laundry", 2.60),
        ("Laundry Detergent 5kg", "Laundry", 11.50),
        ("Laundry Bar Soap 4-pack", "Laundry", 1.80),
        ("Dishwashing Liquid 750ml", "Kitchen Cleaning", 1.45),
        ("Scouring Sponges 5-pack", "Kitchen Cleaning", 0.95),
        ("Bleach 1L", "Cleaning", 1.10),
        ("Floor Cleaner 1L", "Cleaning", 1.95),
        ("Air Freshener 300ml", "Cleaning", 2.30),
        ("Toilet Paper 10 Rolls", "Paper Goods", 4.20),
        ("Kitchen Towels 2 Rolls", "Paper Goods", 1.75),
        ("Garbage Bags 30-pack", "Storage & Bags", 1.90),
        ("Aluminium Foil 10m", "Storage & Bags", 1.65),
        ("Cling Film 30m", "Storage & Bags", 1.40),
        ("Water Jerrycan 20L", "Storage & Bags", 3.80),
        ("Charcoal 10kg", "Cooking Fuel", 4.50),
        ("Matches 10-pack", "Cooking Fuel", 0.50),
        ("Insect Spray 400ml", "Pest Control", 3.10),
        ("Mosquito Coils 10-pack", "Pest Control", 1.20),
        ("Broom", "Cleaning Tools", 2.80),
        ("Mop and Bucket Set", "Cleaning Tools", 9.90),
    ]),
    "Personal Care": ("PRC", 0.8, [
        ("Toothpaste 100ml", "Oral Care", 1.60),
        ("Toothbrush 2-pack", "Oral Care", 1.30),
        ("Miswak Sticks 5-pack", "Oral Care", 0.80),
        ("Bath Soap 3-pack", "Bath & Body", 1.90),
        ("Shower Gel 400ml", "Bath & Body", 3.20),
        ("Shampoo 400ml", "Hair Care", 3.60),
        ("Hair Oil 200ml", "Hair Care", 2.40),
        ("Henna Powder 100g", "Hair Care", 1.15),
        ("Body Lotion 400ml", "Skin Care", 3.90),
        ("Petroleum Jelly 250ml", "Skin Care", 1.70),
        ("Sunscreen SPF50 100ml", "Skin Care", 6.50),
        ("Deodorant Spray 150ml", "Fragrance", 2.90),
        ("Oud Perfume Oil 12ml", "Fragrance", 8.50),
        ("Uunsi Incense 250g", "Fragrance", 4.00),
        ("Razor Blades 5-pack", "Shaving", 2.10),
        ("Shaving Foam 200ml", "Shaving", 2.25),
        ("Baby Diapers 40-pack", "Baby Care", 9.80),
        ("Baby Wipes 80-pack", "Baby Care", 2.40),
        ("Sanitary Pads 10-pack", "Feminine Care", 1.50),
        ("Hand Sanitizer 250ml", "Health", 1.80),
    ]),
    "Electronics": ("ELC", 0.4, [
        ("USB-C Phone Charger", "Phone Accessories", 4.50),
        ("Micro-USB Cable 1m", "Phone Accessories", 2.00),
        ("USB-C Cable 1m", "Phone Accessories", 2.50),
        ("Screen Protector", "Phone Accessories", 1.50),
        ("Power Bank 10000mAh", "Phone Accessories", 14.00),
        ("Feature Phone Dual SIM", "Mobile Phones", 19.00),
        ("Smartphone 64GB", "Mobile Phones", 95.00),
        ("Wired Earphones", "Audio", 3.50),
        ("Bluetooth Earbuds", "Audio", 18.00),
        ("Bluetooth Speaker", "Audio", 22.00),
        ("AA Batteries 4-pack", "Batteries", 2.20),
        ("AAA Batteries 4-pack", "Batteries", 2.20),
        ("LED Bulb 9W", "Lighting", 1.60),
        ("Rechargeable LED Lantern", "Lighting", 9.50),
        ("Solar Lamp Kit", "Lighting", 24.00),
        ("Extension Lead 4-way", "Power", 6.50),
        ("Electric Kettle 1.7L", "Small Appliances", 15.50),
        ("Hair Clipper", "Small Appliances", 16.00),
        ("Table Fan 16in", "Small Appliances", 21.00),
        ("Memory Card 32GB", "Storage", 6.00),
    ]),
    "Fresh Produce": ("FRP", 1.3, [
        ("Bananas 1kg", "Fruit", 1.20),
        ("Mangoes 1kg", "Fruit", 1.80),
        ("Oranges 1kg", "Fruit", 1.60),
        ("Lemons 500g", "Fruit", 0.90),
        ("Papaya", "Fruit", 1.40),
        ("Watermelon", "Fruit", 2.50),
        ("Tomatoes 1kg", "Vegetables", 1.10),
        ("Onions 1kg", "Vegetables", 0.85),
        ("Potatoes 1kg", "Vegetables", 0.95),
        ("Carrots 1kg", "Vegetables", 1.00),
        ("Green Peppers 500g", "Vegetables", 0.90),
        ("Cabbage", "Vegetables", 0.75),
        ("Garlic 250g", "Vegetables", 0.70),
        ("Fresh Coriander Bunch", "Herbs", 0.35),
        ("Fresh Mint Bunch", "Herbs", 0.35),
        ("Eggs 30-tray", "Eggs", 5.40),
        ("Goat Meat 1kg", "Meat", 7.50),
        ("Camel Meat 1kg", "Meat", 6.80),
        ("Beef 1kg", "Meat", 6.20),
        ("Whole Chicken 1.2kg", "Poultry", 5.90),
    ]),
}
COST_MARGIN_RANGE = (0.58, 0.82)  # unit_cost as a share of the list price
N_INACTIVE_PRODUCTS = 6
DISCONTINUED_FROM = date(2025, 1, 1)  # inactive products stop selling from this date
PRICE_INCREASE_DATE = date(2025, 7, 1)
PRICE_INCREASE_PCT = 8
PRICE_INCREASE_EXEMPT = ("Electronics",)

# --------------------------------------------------------------------------- #
# Customers
# --------------------------------------------------------------------------- #
FIRST_NAMES = (
    "Abdi", "Abdirahman", "Abdullahi", "Ahmed", "Ali", "Ayanle", "Bashir", "Deeq",
    "Farah", "Guled", "Hamse", "Hassan", "Hussein", "Ibrahim", "Ismail", "Jama",
    "Khadar", "Khalid", "Liban", "Mahad", "Mohamed", "Mohamud", "Mukhtar", "Musa",
    "Mustafa", "Omar", "Osman", "Said", "Yusuf", "Zakaria",
    "Amina", "Asha", "Ayan", "Deeqa", "Fadumo", "Faiza", "Filsan", "Halima",
    "Hawa", "Hibo", "Hodan", "Ifrah", "Ikran", "Kaltun", "Khadra", "Ladan",
    "Maryan", "Muna", "Nasra", "Nimco", "Qamar", "Ruqia", "Safiya", "Sagal",
    "Sahra", "Shukri", "Sucaad", "Ubah", "Zamzam", "Ilhan",
)
LAST_NAMES = (
    "Abdi", "Abdillahi", "Adan", "Aden", "Ahmed", "Ali", "Awale", "Barre", "Bulhan",
    "Dahir", "Dirie", "Duale", "Egal", "Elmi", "Farah", "Geedi", "Gelle", "Guled",
    "Hassan", "Hersi", "Hirsi", "Hussein", "Ibrahim", "Ismail", "Jama", "Jibril",
    "Kahin", "Mattan", "Mohamed", "Mohamud", "Muse", "Nur", "Obsiye", "Omar",
    "Osman", "Qalib", "Roble", "Said", "Samatar", "Shire", "Warfa", "Warsame",
    "Yasin", "Yusuf",
)
EMAIL_DOMAINS = ("example.com", "example.net", "example.org", "mail.example.com")
PHONE_PREFIXES = ("+25263", "+25265", "+25266")
EARLY_SIGNUP_SHARE = 0.55  # customers who signed up before the sales period
EARLY_SIGNUP_START = date(2021, 1, 1)
LATE_SIGNUP_END = date(2025, 11, 30)

# --------------------------------------------------------------------------- #
# Orders and order lines
# --------------------------------------------------------------------------- #
ANNUAL_GROWTH_2025 = 1.12
WEEKDAY_FACTORS = (0.95, 0.95, 1.0, 1.25, 1.30, 0.95, 0.90)  # Mon..Sun; Thu-Fri weekend
DECEMBER_FACTOR = 1.25
RAMADAN_FACTOR = 1.30
LAST_TEN_NIGHTS_FACTOR = 1.60
EID_AL_FITR_FACTOR = 0.70  # many shops close for the first days of Eid
PRE_EID_AL_ADHA_FACTOR = 1.40
RAMADAN_PERIODS = (
    (date(2024, 3, 11), date(2024, 4, 9)),
    (date(2025, 3, 1), date(2025, 3, 29)),
)
EID_AL_FITR_DATES = (date(2024, 4, 10), date(2025, 3, 30))
EID_AL_ADHA_DATES = (date(2024, 6, 16), date(2025, 6, 6))

OPENING_HOURS = tuple(range(7, 22))
HOUR_WEIGHTS = (2, 4, 6, 7, 7, 5, 4, 4, 5, 7, 9, 9, 8, 6, 3)

HOME_STORE_RATE = 0.85
CHANNEL_WEIGHTS = {"in_store": 70, "mobile_app": 18, "phone": 12}
PAYMENT_WEIGHTS = {
    "in_store": {"cash": 35, "zaad": 42, "edahab": 17, "card": 6},
    "mobile_app": {"zaad": 60, "edahab": 30, "card": 10},
    "phone": {"zaad": 55, "edahab": 30, "cash": 15},
}
STATUS_WEIGHTS = {"completed": 92, "cancelled": 5, "returned": 3}

LINES_PER_ORDER_WEIGHTS = {1: 30, 2: 30, 3: 20, 4: 12, 5: 8}
RETAIL_QUANTITY_WEIGHTS = {1: 50, 2: 25, 3: 12, 4: 7, 6: 6}
WHOLESALE_QUANTITY_WEIGHTS = {1: 20, 2: 20, 3: 15, 5: 15, 10: 15, 12: 10, 24: 5}
ELECTRONICS_MULTI_UNIT_RATE = 0.10
DISCOUNT_RATE = 0.18
RAMADAN_DISCOUNT_RATE = 0.30
DISCOUNT_PERCENTAGES = (5, 10, 15, 20)
ORPHAN_PRICE_RANGE_CENTS = (50, 1500)


# --------------------------------------------------------------------------- #
# Domain objects
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Store:
    store_id: int
    name: str
    city: str
    region: str
    store_type: str
    opened_date: date
    traffic: float


@dataclass(frozen=True)
class Product:
    product_id: int
    sku: str
    name: str
    category: str
    subcategory: str
    unit_cost_cents: int
    unit_price_cents: int
    is_active: bool
    popularity: float

    def price_on(self, day: date) -> int:
        """Return the shelf price in cents on ``day`` (before or after the 2025 increase)."""
        if day >= PRICE_INCREASE_DATE or self.category in PRICE_INCREASE_EXEMPT:
            return self.unit_price_cents
        return round(self.unit_price_cents * 100 / (100 + PRICE_INCREASE_PCT))


@dataclass(frozen=True)
class Customer:
    customer_id: int
    first_name: str
    last_name: str
    email: str
    phone: str
    city: str
    signup_date: date
    activity: float


@dataclass(frozen=True)
class Order:
    order_id: int
    customer_id: int | None
    store: Store
    order_ts: datetime
    channel: str
    payment_method: str
    status: str
    updated_at: datetime


class WeightedPicker:
    """Draw items with replacement in proportion to fixed weights (O(log n) per draw)."""

    def __init__(self, items: Sequence[Product], weights: Sequence[float]) -> None:
        self._items = list(items)
        self._cumulative = list(accumulate(weights))

    def pick(self, rng: random.Random) -> Product:
        return self._items[bisect_right(self._cumulative, rng.random() * self._cumulative[-1])]


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def weighted_choice(rng: random.Random, weights: Mapping[T, float]) -> T:
    """Pick one key of ``weights`` with probability proportional to its value."""
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def format_money(cents: int) -> str:
    """Render integer cents as a two-decimal USD amount, e.g. 625 -> '6.25'."""
    return f"{cents // 100}.{cents % 100:02d}"


def random_date(rng: random.Random, start: date, end: date) -> date:
    """Uniformly pick a date in the inclusive range [start, end]."""
    return start + timedelta(days=rng.randint(0, (end - start).days))


def is_ramadan(day: date) -> bool:
    return any(start <= day <= end for start, end in RAMADAN_PERIODS)


def day_weight(day: date) -> float:
    """Relative order volume of a calendar day (growth, weekday, religious calendar)."""
    weight = WEEKDAY_FACTORS[day.weekday()]
    if day.year == 2025:
        weight *= ANNUAL_GROWTH_2025
    if day.month == 12:
        weight *= DECEMBER_FACTOR
    for start, end in RAMADAN_PERIODS:
        if start <= day <= end:
            weight *= LAST_TEN_NIGHTS_FACTOR if (end - day).days < 10 else RAMADAN_FACTOR
    if any(0 <= (day - eid).days < 3 for eid in EID_AL_FITR_DATES):
        weight *= EID_AL_FITR_FACTOR
    if any(0 < (eid - day).days <= 7 for eid in EID_AL_ADHA_DATES):
        weight *= PRE_EID_AL_ADHA_FACTOR
    return weight


def interleave(rng: random.Random, rows: list[list[str]], extras: list[list[str]]) -> list[list[str]]:
    """Insert ``extras`` at random positions among ``rows``, keeping ``rows`` in order."""
    keyed = [(float(index), row) for index, row in enumerate(rows)]
    keyed += [(rng.uniform(0, len(rows)), row) for row in extras]
    keyed.sort(key=lambda pair: pair[0])
    return [row for _, row in keyed]


# --------------------------------------------------------------------------- #
# Stores and products
# --------------------------------------------------------------------------- #
def build_stores() -> list[Store]:
    return [Store(*spec) for spec in STORES]


def build_products(rng: random.Random) -> list[Product]:
    """Build the catalogue with cost prices, popularity weights and a few inactive SKUs."""
    products: list[Product] = []
    product_id = FIRST_PRODUCT_ID
    for category, (prefix, category_popularity, items) in CATALOGUE.items():
        for name, subcategory, list_price in items:
            price_cents = round(list_price * 100)
            products.append(Product(
                product_id=product_id,
                sku=f"{prefix}-{product_id:04d}",
                name=name,
                category=category,
                subcategory=subcategory,
                unit_cost_cents=round(price_cents * rng.uniform(*COST_MARGIN_RANGE)),
                unit_price_cents=price_cents,
                is_active=True,
                popularity=category_popularity * rng.uniform(0.5, 1.5) / list_price ** 0.5,
            ))
            product_id += 1
    for index in rng.sample(range(len(products)), N_INACTIVE_PRODUCTS):
        products[index] = replace(products[index], is_active=False)
    return products


def product_row(product: Product) -> list[str]:
    return [
        str(product.product_id), product.sku, product.name, product.category,
        product.subcategory, format_money(product.unit_cost_cents),
        format_money(product.unit_price_cents), "true" if product.is_active else "false",
    ]


def store_row(store: Store) -> list[str]:
    return [
        str(store.store_id), store.name, store.city, store.region,
        store.store_type, store.opened_date.isoformat(),
    ]


# --------------------------------------------------------------------------- #
# Customers
# --------------------------------------------------------------------------- #
def random_signup_date(rng: random.Random) -> date:
    if rng.random() < EARLY_SIGNUP_SHARE:
        return random_date(rng, EARLY_SIGNUP_START, PERIOD_START - timedelta(days=1))
    return random_date(rng, PERIOD_START, LATE_SIGNUP_END)


def unique_value(rng: random.Random, make: Callable[[random.Random], str], used: set[str]) -> str:
    """Call ``make(rng)`` until it returns a value not yet in ``used``; record it."""
    value = make(rng)
    while value in used:
        value = make(rng)
    used.add(value)
    return value


def build_customers(rng: random.Random, stores: list[Store]) -> list[Customer]:
    """Create distinct customers; ids increase with signup date like a real CRM."""
    cities = [store.city for store in stores]
    city_weights = [store.traffic for store in stores]
    used_emails: set[str] = set()
    used_phones: set[str] = set()
    profiles = []
    for _ in range(N_CUSTOMERS):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        local = f"{first}.{last}".lower()
        email = unique_value(
            rng,
            lambda r: f"{local}{r.choice(['', str(r.randint(1, 99))])}@{r.choice(EMAIL_DOMAINS)}",
            used_emails,
        )
        phone = unique_value(
            rng, lambda r: f"{r.choice(PHONE_PREFIXES)}{r.randint(0, 9_999_999):07d}", used_phones
        )
        city = rng.choices(cities, weights=city_weights)[0]
        profiles.append((random_signup_date(rng), first, last, email, phone, city,
                         rng.lognormvariate(0, 0.8)))
    profiles.sort(key=lambda profile: profile[0])
    return [
        Customer(FIRST_CUSTOMER_ID + index, first, last, email, phone, city, signup, activity)
        for index, (signup, first, last, email, phone, city, activity) in enumerate(profiles)
    ]


def messy_name(rng: random.Random, name: str) -> str:
    """Return the name in the casing a hurried cashier might have typed."""
    roll = rng.random()
    if roll < NAME_UPPER_RATE:
        return name.upper()
    if roll < NAME_UPPER_RATE + NAME_LOWER_RATE:
        return name.lower()
    return name


def messy_email(rng: random.Random, email: str) -> str:
    """Return the email as captured at the till: sometimes empty, cased or padded."""
    if rng.random() < EMPTY_EMAIL_RATE:
        return ""
    roll = rng.random()
    if roll < EMAIL_UPPER_RATE:
        email = email.upper()
    elif roll < EMAIL_UPPER_RATE + EMAIL_TITLE_CASE_RATE:
        local, domain = email.split("@")
        email = ".".join(part.capitalize() for part in local.split(".")) + "@" + domain.capitalize()
    if rng.random() < EMAIL_WHITESPACE_RATE:
        email = rng.choice((f" {email}", f"{email} ", f" {email} "))
    return email


def customer_rows(rng: random.Random, customers: list[Customer]) -> list[list[str]]:
    """Render customers with messy values and shuffle in exact duplicate rows."""
    rows = [
        [
            str(customer.customer_id),
            messy_name(rng, customer.first_name),
            messy_name(rng, customer.last_name),
            messy_email(rng, customer.email),
            customer.phone,
            customer.city,
            customer.signup_date.isoformat(),
        ]
        for customer in customers
    ]
    n_duplicates = round(len(rows) * CUSTOMER_DUPLICATE_RATE)
    duplicates = [list(row) for row in rng.sample(rows, n_duplicates)]
    return interleave(rng, rows, duplicates)


# --------------------------------------------------------------------------- #
# Orders
# --------------------------------------------------------------------------- #
def order_timestamps(rng: random.Random) -> list[datetime]:
    """Draw N_ORDERS timestamps following the seasonal day and hour profiles."""
    days = [PERIOD_START + timedelta(days=offset)
            for offset in range((PERIOD_END - PERIOD_START).days + 1)]
    chosen_days = rng.choices(days, weights=[day_weight(day) for day in days], k=N_ORDERS)
    timestamps = [
        datetime.combine(day, time(
            rng.choices(OPENING_HOURS, weights=HOUR_WEIGHTS)[0],
            rng.randint(0, 59),
            rng.randint(0, 59),
        ))
        for day in chosen_days
    ]
    return sorted(timestamps)


class CustomerPicker:
    """Pick a registered customer who had already signed up on a given day."""

    def __init__(self, customers: list[Customer]) -> None:
        self._customers = customers  # sorted by signup_date
        self._signup_dates = [customer.signup_date for customer in customers]
        self._cumulative = list(accumulate(customer.activity for customer in customers))

    def pick(self, rng: random.Random, day: date) -> Customer | None:
        eligible = bisect_right(self._signup_dates, day)
        if eligible == 0:
            return None
        target = rng.random() * self._cumulative[eligible - 1]
        return self._customers[bisect_right(self._cumulative, target, hi=eligible - 1)]


def final_updated_at(rng: random.Random, status: str, order_ts: datetime) -> datetime:
    """When the source system last touched an order with the given final status."""
    if status == "cancelled":
        return order_ts + timedelta(minutes=rng.randint(5, 48 * 60))
    if status == "returned":
        return order_ts + timedelta(days=rng.randint(1, 14), minutes=rng.randint(0, 600))
    return order_ts


def build_orders(rng: random.Random, stores: list[Store], customers: list[Customer]) -> list[Order]:
    """Create the distinct orders, ids increasing with order time."""
    store_by_city = {store.city: store for store in stores}
    store_weights = [store.traffic for store in stores]
    customer_picker = CustomerPicker(customers)
    orders = []
    for offset, order_ts in enumerate(order_timestamps(rng)):
        customer = None
        if rng.random() >= WALK_IN_RATE:
            customer = customer_picker.pick(rng, order_ts.date())
        if customer is not None and rng.random() < HOME_STORE_RATE:
            store = store_by_city[customer.city]
        else:
            store = rng.choices(stores, weights=store_weights)[0]
        channel = "in_store" if customer is None else weighted_choice(rng, CHANNEL_WEIGHTS)
        status = weighted_choice(rng, STATUS_WEIGHTS)
        orders.append(Order(
            order_id=FIRST_ORDER_ID + offset,
            customer_id=None if customer is None else customer.customer_id,
            store=store,
            order_ts=order_ts,
            channel=channel,
            payment_method=weighted_choice(rng, PAYMENT_WEIGHTS[channel]),
            status=status,
            updated_at=final_updated_at(rng, status, order_ts),
        ))
    return orders


def order_row(order: Order) -> list[str]:
    return [
        str(order.order_id),
        "" if order.customer_id is None else str(order.customer_id),
        str(order.store.store_id),
        order.order_ts.isoformat(),
        order.channel,
        order.payment_method,
        order.status,
        order.updated_at.isoformat(),
    ]


def order_rows(rng: random.Random, orders: list[Order]) -> list[list[str]]:
    """Render orders and add the stale first version of ~1% of orders the source re-sent.

    Only orders that were later cancelled or returned are re-sent; their stale row
    still says ``completed`` with ``updated_at = order_ts``. Stale rows land at random
    positions, so file order cannot be used to pick the winner.
    """
    changed = [order for order in orders if order.status != "completed"]
    resent = rng.sample(changed, round(len(orders) * RESENT_ORDER_RATE))
    stale = [order_row(replace(order, status="completed", updated_at=order.order_ts))
             for order in resent]
    return interleave(rng, [order_row(order) for order in orders], stale)


# --------------------------------------------------------------------------- #
# Order items
# --------------------------------------------------------------------------- #
def line_quantity(rng: random.Random, product: Product | None, store_type: str) -> int:
    if product is not None and product.category == "Electronics":
        return 2 if rng.random() < ELECTRONICS_MULTI_UNIT_RATE else 1
    if store_type == "Wholesale":
        return weighted_choice(rng, WHOLESALE_QUANTITY_WEIGHTS)
    return weighted_choice(rng, RETAIL_QUANTITY_WEIGHTS)


def line_discount(rng: random.Random, line_total_cents: int, day: date) -> int:
    """Percentage discount in cents; floor division keeps it <= the line total."""
    rate = RAMADAN_DISCOUNT_RATE if is_ramadan(day) else DISCOUNT_RATE
    if rng.random() >= rate:
        return 0
    return line_total_cents * rng.choice(DISCOUNT_PERCENTAGES) // 100


def pick_basket(rng: random.Random, picker: WeightedPicker, size: int) -> list[Product]:
    """Pick ``size`` distinct products for one order."""
    basket: list[Product] = []
    while len(basket) < size:
        product = picker.pick(rng)
        if product not in basket:
            basket.append(product)
    return basket


def order_item_rows(rng: random.Random, orders: list[Order], products: list[Product]) -> list[list[str]]:
    """Create 1-5 lines per order, priced as on the order date."""
    active = [product for product in products if product.is_active]
    all_time_picker = WeightedPicker(products, [product.popularity for product in products])
    active_picker = WeightedPicker(active, [product.popularity for product in active])
    rows = []
    for order in orders:
        day = order.order_ts.date()
        picker = all_time_picker if day < DISCONTINUED_FROM else active_picker
        size = weighted_choice(rng, LINES_PER_ORDER_WEIGHTS)
        for product in pick_basket(rng, picker, size):
            if rng.random() < ORPHAN_ITEM_RATE:
                product_id = rng.choice(ORPHAN_PRODUCT_IDS)
                price_cents = rng.randint(*ORPHAN_PRICE_RANGE_CENTS)
                quantity = line_quantity(rng, None, order.store.store_type)
            else:
                product_id = product.product_id
                price_cents = product.price_on(day)
                quantity = line_quantity(rng, product, order.store.store_type)
            discount_cents = line_discount(rng, quantity * price_cents, day)
            rows.append([
                str(len(rows) + 1),
                str(order.order_id),
                str(product_id),
                str(quantity),
                format_money(price_cents),
                format_money(discount_cents) if discount_cents else "",
            ])
    return rows


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def write_csv(path: Path, header: Sequence[str], rows: list[list[str]]) -> int:
    """Write a UTF-8, comma-separated file with a header row; return the data row count."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    return len(rows)


def generate(out_dir: Path, seed: int = SEED) -> dict[str, int]:
    """Generate all five extracts into ``out_dir``; return data rows per file name."""
    rng = random.Random(seed)
    stores = build_stores()
    products = build_products(rng)
    customers = build_customers(rng, stores)
    orders = build_orders(rng, stores, customers)

    files = {
        "stores.csv": (STORE_COLUMNS, [store_row(store) for store in stores]),
        "products.csv": (PRODUCT_COLUMNS, [product_row(product) for product in products]),
        "customers.csv": (CUSTOMER_COLUMNS, customer_rows(rng, customers)),
        "orders.csv": (ORDER_COLUMNS, order_rows(rng, orders)),
        "order_items.csv": (ORDER_ITEM_COLUMNS, order_item_rows(rng, orders, products)),
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    return {name: write_csv(out_dir / name, header, rows) for name, (header, rows) in files.items()}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Dukaan Retail's synthetic CSV extracts.")
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT_DIR,
        help="output directory (default: data/raw in the repository)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    counts = generate(args.out)
    for name, count in counts.items():
        print(f"{name:<16} {count:>7,} rows")
    print(f"Wrote {len(counts)} files to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
