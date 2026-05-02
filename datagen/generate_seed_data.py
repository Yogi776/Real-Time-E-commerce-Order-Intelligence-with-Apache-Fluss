#!/usr/bin/env python3
"""
Seed Data Generator for E-commerce Order Intelligence Platform

Generates realistic customer profiles and product catalog data,
outputting Flink SQL INSERT statements for Fluss tables.

Usage:
    python datagen/generate_seed_data.py > sql/03_seed_data.sql
    python datagen/generate_seed_data.py --customers 50 --products 100
"""

import argparse
import hashlib
import random
from datetime import date, datetime, timedelta
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Reference data pools - sourced from real Indian market demographics
# ---------------------------------------------------------------------------

FIRST_NAMES_MALE = [
    "Aarav", "Rahul", "Amit", "Vikram", "Rohit", "Arjun", "Saurabh",
    "Nikhil", "Karthik", "Rajesh", "Aditya", "Harsh", "Manish", "Pranav",
    "Siddharth", "Vivek", "Gaurav", "Abhishek", "Dhruv", "Yash",
    "Rohan", "Kunal", "Ankit", "Varun", "Ishaan",
]

FIRST_NAMES_FEMALE = [
    "Priya", "Sneha", "Deepika", "Ananya", "Meera", "Kavitha", "Pooja",
    "Ritu", "Neha", "Simran", "Divya", "Shruti", "Nisha", "Tanvi",
    "Aisha", "Swati", "Jyoti", "Pallavi", "Radhika", "Megha",
    "Kriti", "Aditi", "Isha", "Sakshi", "Trisha",
]

LAST_NAMES = {
    "North": ["Sharma", "Verma", "Kumar", "Singh", "Gupta", "Agarwal",
              "Saxena", "Kaur", "Mehta", "Kapoor", "Malhotra", "Bhatia"],
    "South": ["Nair", "Iyer", "Reddy", "Rao", "Menon", "Pillai",
              "Krishnan", "Subramanian", "Naidu", "Hegde"],
    "West":  ["Patel", "Joshi", "Deshmukh", "Jain", "Shah", "Parekh",
              "Kulkarni", "Patil", "Deshpande", "Thakkar"],
    "East":  ["Banerjee", "Mukherjee", "Das", "Sen", "Bose", "Roy",
              "Chatterjee", "Ghosh"],
}

CITIES_WITH_REGION_AND_WEIGHT = [
    ("Mumbai",     "West",  20),
    ("Delhi",      "North", 18),
    ("Bengaluru",  "South", 15),
    ("Pune",       "West",  10),
    ("Hyderabad",  "South", 10),
    ("Chennai",    "South", 10),
    ("Ahmedabad",  "West",   9),
    ("Kolkata",    "East",   8),
]

LOYALTY_TIERS_WEIGHTED = [
    ("BRONZE",   40),
    ("SILVER",   30),
    ("GOLD",     20),
    ("PLATINUM", 10),
]

# Product catalog with realistic Indian MRP and inventory
PRODUCTS = [
    # Electronics
    ("SKU-ELEC-1001", "iPhone 15 Pro 128GB",               "electronics", "Apple",            79999.00, (60, 100)),
    ("SKU-ELEC-1002", "Samsung Galaxy S24 Ultra",           "electronics", "Samsung",          69999.00, (80, 150)),
    ("SKU-ELEC-1003", "Sony WH-1000XM5 Headphones",        "electronics", "Sony",             24999.00, (120, 200)),
    ("SKU-ELEC-1004", "MacBook Air M3 13-inch",             "electronics", "Apple",           114999.00, (30, 60)),
    ("SKU-ELEC-1005", "iPad Air 5th Gen 64GB",              "electronics", "Apple",            54999.00, (70, 120)),
    ("SKU-ELEC-1006", "JBL Flip 6 Bluetooth Speaker",       "electronics", "JBL",              8999.00, (180, 250)),
    # Fashion
    ("SKU-FASH-2001", "Levis 501 Original Fit Jeans",       "fashion", "Levis",                3499.00, (250, 400)),
    ("SKU-FASH-2002", "Nike Air Max 270 Running Shoes",     "fashion", "Nike",                 8999.00, (180, 280)),
    ("SKU-FASH-2003", "Allen Solly Slim Fit Formal Shirt",  "fashion", "Allen Solly",          1499.00, (400, 550)),
    ("SKU-FASH-2004", "Zara Pleated Midi Dress",            "fashion", "Zara",                 2999.00, (150, 230)),
    ("SKU-FASH-2005", "Ray-Ban Aviator Classic Sunglasses", "fashion", "Ray-Ban",              5999.00, (250, 370)),
    ("SKU-FASH-2006", "Woodland Trekking Boots",            "fashion", "Woodland",             4499.00, (200, 300)),
    # Grocery
    ("SKU-GROC-3001", "Tata Tea Premium 1kg",               "grocery", "Tata",                  399.00, (2000, 3000)),
    ("SKU-GROC-3002", "Aashirvaad Superior MP Atta 10kg",   "grocery", "Aashirvaad",            549.00, (1500, 2200)),
    ("SKU-GROC-3003", "Fortune Rice Bran Oil 5L",           "grocery", "Fortune",               899.00, (1000, 1500)),
    ("SKU-GROC-3004", "Maggi 2-Minute Noodles Family Pack", "grocery", "Nestle",                299.00, (3000, 4000)),
    ("SKU-GROC-3005", "Amul Butter 500g",                   "grocery", "Amul",                  275.00, (3500, 5000)),
    # Books
    ("SKU-BOOK-4001", "Atomic Habits by James Clear",       "books", "Penguin",                 399.00, (350, 550)),
    ("SKU-BOOK-4002", "The Psychology of Money",            "books", "Jaico",                   349.00, (300, 450)),
    ("SKU-BOOK-4003", "Ikigai The Japanese Secret",         "books", "Penguin",                 299.00, (400, 600)),
    ("SKU-BOOK-4004", "Rich Dad Poor Dad",                  "books", "Plata",                   399.00, (350, 500)),
    ("SKU-BOOK-4005", "Sapiens A Brief History",            "books", "Vintage",                 499.00, (250, 380)),
    # Beauty
    ("SKU-BEAU-5001", "Lakme Eyeconic Kajal Deep Black",    "beauty", "Lakme",                  249.00, (700, 1000)),
    ("SKU-BEAU-5002", "Nivea Cocoa Nourish Body Lotion",    "beauty", "Nivea",                  399.00, (900, 1300)),
    ("SKU-BEAU-5003", "Philips BT3211 Beard Trimmer",       "beauty", "Philips",               1299.00, (400, 650)),
    ("SKU-BEAU-5004", "Forest Essentials Soundarya Wash",   "beauty", "Forest Essentials",     1175.00, (550, 800)),
    # Sports
    ("SKU-SPRT-6001", "Yonex Nanoray Light 18i Racket",     "sports", "Yonex",                 2999.00, (140, 220)),
    ("SKU-SPRT-6002", "Nike Premier League Flight Football", "sports", "Nike",                  1499.00, (200, 300)),
    ("SKU-SPRT-6003", "Boldfit Resistance Bands Set",       "sports", "Boldfit",                599.00, (350, 500)),
    ("SKU-SPRT-6004", "Strauss Premium Yoga Mat 6mm",       "sports", "Strauss",                899.00, (280, 400)),
]


def _deterministic_hex_id(seed: str) -> str:
    """Generate a deterministic 8-char hex ID from a seed string."""
    return hashlib.sha256(seed.encode()).hexdigest()[:8]


def _weighted_choice(options_with_weights: list) -> str:
    """Pick a random option using weights."""
    options, weights = zip(*options_with_weights)
    return random.choices(options, weights=weights, k=1)[0]


def _pick_city() -> tuple[str, str]:
    """Return (city, region) weighted by e-commerce penetration."""
    cities, regions, weights = zip(*CITIES_WITH_REGION_AND_WEIGHT)
    idx = random.choices(range(len(cities)), weights=weights, k=1)[0]
    return cities[idx], regions[idx]


def _random_signup_date(tier: str) -> date:
    """PLATINUM/GOLD customers signed up earlier (loyal), BRONZE later (new)."""
    today = date.today()
    if tier == "PLATINUM":
        days_ago = random.randint(700, 900)
    elif tier == "GOLD":
        days_ago = random.randint(500, 750)
    elif tier == "SILVER":
        days_ago = random.randint(300, 550)
    else:
        days_ago = random.randint(60, 350)
    return today - timedelta(days=days_ago)


def generate_customers(count: int, seed: int = 42) -> list[dict]:
    """Generate realistic customer profiles."""
    random.seed(seed)
    customers = []

    tier_pool = []
    for tier, weight in LOYALTY_TIERS_WEIGHTED:
        tier_pool.extend([tier] * weight)

    for i in range(count):
        is_female = i % 2 == 1
        first_name = random.choice(FIRST_NAMES_FEMALE if is_female else FIRST_NAMES_MALE)

        city, region = _pick_city()
        last_name = random.choice(LAST_NAMES[region])

        tier = random.choice(tier_pool)
        signup = _random_signup_date(tier)

        customer_id = f"CUS-{_deterministic_hex_id(f'customer-{i}-{first_name}-{last_name}')}"

        customers.append({
            "customer_id": customer_id,
            "customer_name": f"{first_name} {last_name}",
            "city": city,
            "loyalty_tier": tier,
            "signup_date": signup,
            "updated_at": datetime.combine(signup, datetime.min.time().replace(hour=10, minute=30)),
        })

    return customers


def generate_products(seed: int = 42) -> list[dict]:
    """Generate product catalog from the reference data with randomized inventory."""
    random.seed(seed)
    products = []
    base_date = datetime(2024, 1, 1)

    for sku, name, category, brand, price, (inv_min, inv_max) in PRODUCTS:
        products.append({
            "product_id": sku,
            "product_name": name,
            "category": category,
            "brand": brand,
            "unit_price": price,
            "inventory_count": random.randint(inv_min, inv_max),
            "updated_at": base_date,
        })

    return products


def _escape_sql(value: str) -> str:
    """Escape single quotes for SQL strings."""
    return value.replace("'", "''")


def emit_sql(customers: list[dict], products: list[dict]) -> str:
    """Generate the complete SQL file content."""
    lines = []
    lines.append("-- =============================================================================")
    lines.append("-- 03_seed_data.sql")
    lines.append("-- Auto-generated by datagen/generate_seed_data.py")
    lines.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"-- Customers: {len(customers)}, Products: {len(products)}")
    lines.append("-- =============================================================================")
    lines.append("")
    lines.append("USE CATALOG fluss_catalog;")
    lines.append("USE ecommerce;")
    lines.append("")
    lines.append("SET 'execution.runtime-mode' = 'batch';")
    lines.append("SET 'table.dml-sync' = 'true';")
    lines.append("")

    # Customers
    lines.append("-- =============================================================================")
    lines.append(f"-- CUSTOMER PROFILES ({len(customers)} records)")
    lines.append("-- =============================================================================")
    lines.append("")
    lines.append("INSERT INTO customer_profile VALUES")

    cust_rows = []
    for c in customers:
        row = (
            f"    ('{_escape_sql(c['customer_id'])}', "
            f"'{_escape_sql(c['customer_name'])}', "
            f"'{c['city']}', "
            f"'{c['loyalty_tier']}', "
            f"DATE '{c['signup_date'].isoformat()}', "
            f"TIMESTAMP '{c['updated_at'].strftime('%Y-%m-%d %H:%M:%S')}')"
        )
        cust_rows.append(row)
    lines.append(",\n".join(cust_rows) + ";")
    lines.append("")

    # Products
    lines.append("-- =============================================================================")
    lines.append(f"-- PRODUCT CATALOG ({len(products)} records)")
    lines.append("-- =============================================================================")
    lines.append("")
    lines.append("INSERT INTO product_catalog VALUES")

    prod_rows = []
    for p in products:
        row = (
            f"    ('{_escape_sql(p['product_id'])}', "
            f"'{_escape_sql(p['product_name'])}', "
            f"'{p['category']}', "
            f"'{_escape_sql(p['brand'])}', "
            f"{p['unit_price']:.2f}, "
            f"{p['inventory_count']}, "
            f"TIMESTAMP '{p['updated_at'].strftime('%Y-%m-%d %H:%M:%S')}')"
        )
        prod_rows.append(row)
    lines.append(",\n".join(prod_rows) + ";")

    return "\n".join(lines)


def emit_customer_ids(customers: list[dict]) -> str:
    """Output customer IDs for use in flink-faker Options.option expressions."""
    ids = [c["customer_id"] for c in customers]
    return ",".join(f"''{cid}''" for cid in ids)


def emit_product_ids(products: list[dict]) -> str:
    """Output product IDs for use in flink-faker Options.option expressions."""
    ids = [p["product_id"] for p in products]
    return ",".join(f"''{pid}''" for pid in ids)


def main():
    parser = argparse.ArgumentParser(description="Generate seed data for e-commerce platform")
    parser.add_argument("--customers", type=int, default=20, help="Number of customers (default: 20)")
    parser.add_argument("--products", action="store_true", help="Use full 30-product catalog (always on)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--output", type=str, default=None, help="Output file path (default: stdout)")
    parser.add_argument("--emit-ids", action="store_true", help="Print customer/product IDs for faker config")
    args = parser.parse_args()

    customers = generate_customers(args.customers, seed=args.seed)
    products = generate_products(seed=args.seed)

    if args.emit_ids:
        print("-- Customer IDs for flink-faker Options.option:")
        print(f"-- {emit_customer_ids(customers)}")
        print()
        print("-- Product IDs for flink-faker Options.option:")
        print(f"-- {emit_product_ids(products)}")
        return

    sql_content = emit_sql(customers, products)

    if args.output:
        with open(args.output, "w") as f:
            f.write(sql_content + "\n")
        print(f"Generated {args.output} ({len(customers)} customers, {len(products)} products)")
    else:
        print(sql_content)


if __name__ == "__main__":
    main()
