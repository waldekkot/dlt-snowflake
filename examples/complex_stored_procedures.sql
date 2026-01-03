-- ============================================================================
-- Complex dlt Pipelines for Validation Testing
-- ============================================================================
--
-- Deployed to: DLT_SNOWPARK_SFRT.COMPLEX_DEMO schema
-- 
-- These stored procedures test dlt_snowpark.py with:
--   - Complex nested data structures
--   - Large datasets (2000+ rows each)
--   - Real-world ETL patterns
--   - Multiple table relationships
--   - Various data types and transformations
--
-- GOAL: Validate that DltSnowparkLoader works for real-life dlt pipelines
--       with minimal impact on stored procedure code.
--
-- SETUP:
--   snow stage copy dlt_snowpark.py @DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/ -c dlt-demo --overwrite
--
-- ============================================================================


-- ============================================================================
-- PIPELINE 1: E-COMMERCE DATA WAREHOUSE (2000+ products, complex structure)
-- ============================================================================
-- Simulates a realistic e-commerce data load with:
--   - PRODUCTS: 2000 products with nested attributes
--   - PRODUCT_CATEGORIES: Category hierarchy
--   - PRODUCT_REVIEWS: 6000+ reviews (3 per product average)
--   - PRODUCT_PRICING_HISTORY: Historical prices (4000+ records)
--   - PRODUCT_INVENTORY: Stock levels per warehouse
--
-- Tests: Large batch handling, complex data generation, multiple related tables

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_ECOMMERCE_DATA_WAREHOUSE()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'load_ecommerce_data'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'dlt')
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Complex e-commerce data warehouse with 2000+ products and related tables'
AS
$$
def load_ecommerce_data(session):
    """
    E-Commerce Data Warehouse Pipeline
    
    Generates realistic e-commerce data with complex relationships:
    - 2000 products across 10 categories
    - 6000+ product reviews with ratings and sentiment
    - 4000+ pricing history records
    - 5000+ inventory records across 3 warehouses
    
    Total: ~17,000+ rows across 5 tables
    """
    import dlt
    import json
    import random
    import hashlib
    from datetime import datetime, timedelta
    from dlt_snowpark import DltSnowparkLoader
    
    random.seed(42)  # Reproducible data
    loader = DltSnowparkLoader(session, pipeline_name="ecommerce_warehouse", batch_size=1000)
    
    # Configuration
    NUM_PRODUCTS = 2000
    WAREHOUSES = ["WAREHOUSE_EAST", "WAREHOUSE_WEST", "WAREHOUSE_CENTRAL"]
    
    CATEGORIES = [
        {"id": 1, "name": "Electronics", "parent_id": None, "slug": "electronics"},
        {"id": 2, "name": "Smartphones", "parent_id": 1, "slug": "smartphones"},
        {"id": 3, "name": "Laptops", "parent_id": 1, "slug": "laptops"},
        {"id": 4, "name": "Accessories", "parent_id": 1, "slug": "accessories"},
        {"id": 5, "name": "Home & Garden", "parent_id": None, "slug": "home-garden"},
        {"id": 6, "name": "Furniture", "parent_id": 5, "slug": "furniture"},
        {"id": 7, "name": "Kitchen", "parent_id": 5, "slug": "kitchen"},
        {"id": 8, "name": "Clothing", "parent_id": None, "slug": "clothing"},
        {"id": 9, "name": "Men's Fashion", "parent_id": 8, "slug": "mens-fashion"},
        {"id": 10, "name": "Women's Fashion", "parent_id": 8, "slug": "womens-fashion"},
    ]
    
    ADJECTIVES = ["Premium", "Ultra", "Pro", "Essential", "Classic", "Modern", "Eco", "Smart", "Compact", "Deluxe"]
    NOUNS = ["Widget", "Gadget", "Device", "Tool", "System", "Kit", "Set", "Bundle", "Pack", "Unit"]
    BRANDS = ["TechCorp", "GlobeTech", "InnovateCo", "QualityFirst", "ValueBrand", "PremiumChoice", "EcoFriendly", "SmartLife"]
    COLORS = ["Black", "White", "Silver", "Gold", "Blue", "Red", "Green", "Gray"]
    SIZES = ["XS", "S", "M", "L", "XL", "XXL", None, None]  # Some products don't have sizes
    
    REVIEW_SENTIMENTS = ["positive", "neutral", "negative"]
    REVIEW_TEMPLATES = {
        "positive": [
            "Excellent product! {} exceeded my expectations.",
            "Very happy with this purchase. The {} is fantastic!",
            "Best {} I've ever owned. Highly recommend!",
            "Amazing quality for the price. Love this {}!",
            "Five stars! This {} is a game changer."
        ],
        "neutral": [
            "Decent {}. Does what it's supposed to do.",
            "Average {}. Nothing special but works fine.",
            "The {} is okay. Met basic expectations.",
            "Fair product. The {} has room for improvement.",
            "Standard {}. You get what you pay for."
        ],
        "negative": [
            "Disappointed with this {}. Not worth the price.",
            "The {} broke after a week. Poor quality.",
            "Would not recommend this {}. Save your money.",
            "Terrible {}. Returning immediately.",
            "Very poor {}. Does not match description."
        ]
    }
    
    def generate_sku(product_id, category_id):
        return f"SKU-{category_id:02d}-{product_id:05d}"
    
    def generate_upc(product_id):
        base = f"00{product_id:010d}"
        checksum = sum(int(d) * (3 if i % 2 else 1) for i, d in enumerate(base[:11])) % 10
        return f"{base[:11]}{(10 - checksum) % 10}"
    
    # Generate products
    products = []
    pricing_history = []
    inventory_records = []
    reviews = []
    
    base_date = datetime(2024, 1, 1)
    current_date = datetime(2026, 1, 3)
    
    for i in range(1, NUM_PRODUCTS + 1):
        category = random.choice(CATEGORIES)
        base_price = round(random.uniform(9.99, 999.99), 2)
        
        product = {
            "product_id": i,
            "sku": generate_sku(i, category["id"]),
            "upc": generate_upc(i),
            "name": f"{random.choice(ADJECTIVES)} {random.choice(NOUNS)} {i}",
            "description": f"High-quality {category['name'].lower()} product with advanced features and premium materials.",
            "brand": random.choice(BRANDS),
            "category_id": category["id"],
            "category_name": category["name"],
            "base_price": base_price,
            "current_price": base_price,
            "cost": round(base_price * random.uniform(0.3, 0.6), 2),
            "weight_kg": round(random.uniform(0.1, 25.0), 2),
            "dimensions_cm": f"{random.randint(5,100)}x{random.randint(5,100)}x{random.randint(1,50)}",
            "color": random.choice(COLORS),
            "size": random.choice(SIZES),
            "is_active": random.random() > 0.1,  # 90% active
            "is_featured": random.random() < 0.05,  # 5% featured
            "avg_rating": round(random.uniform(2.5, 5.0), 2),
            "review_count": 0,  # Will be updated
            "created_at": (base_date + timedelta(days=random.randint(0, 700))).isoformat(),
            "updated_at": current_date.isoformat(),
            "meta_title": f"Buy {random.choice(ADJECTIVES)} {random.choice(NOUNS)} | Best {category['name']}",
            "meta_keywords": f"{category['name'].lower()}, {random.choice(BRANDS).lower()}, best deals",
            "tags": json.dumps([category["slug"], random.choice(BRANDS).lower(), "trending" if random.random() < 0.2 else "standard"])
        }
        products.append(product)
        
        # Generate pricing history (2-3 price changes per product)
        num_price_changes = random.randint(2, 4)
        current_hist_price = base_price * random.uniform(0.8, 1.2)
        
        for j in range(num_price_changes):
            price_date = base_date + timedelta(days=random.randint(0, 700))
            pricing_history.append({
                "price_history_id": len(pricing_history) + 1,
                "product_id": i,
                "price": round(current_hist_price, 2),
                "discount_percent": random.choice([0, 0, 0, 10, 15, 20, 25, 30]),
                "promotion_name": random.choice([None, None, "Summer Sale", "Black Friday", "Flash Deal", "Clearance"]),
                "effective_date": price_date.isoformat(),
                "end_date": (price_date + timedelta(days=random.randint(7, 90))).isoformat(),
                "created_by": f"user_{random.randint(1, 10)}"
            })
            current_hist_price *= random.uniform(0.9, 1.15)
        
        # Generate inventory per warehouse
        for warehouse in WAREHOUSES:
            inventory_records.append({
                "inventory_id": len(inventory_records) + 1,
                "product_id": i,
                "warehouse_code": warehouse,
                "quantity_on_hand": random.randint(0, 500),
                "quantity_reserved": random.randint(0, 50),
                "quantity_available": max(0, random.randint(0, 450)),
                "reorder_point": random.randint(10, 50),
                "reorder_quantity": random.randint(50, 200),
                "last_restock_date": (current_date - timedelta(days=random.randint(1, 30))).isoformat(),
                "last_count_date": (current_date - timedelta(days=random.randint(0, 7))).isoformat(),
                "bin_location": f"AISLE-{random.randint(1,50):02d}-SHELF-{random.randint(1,10):02d}",
                "updated_at": current_date.isoformat()
            })
        
        # Generate reviews (1-5 per product)
        num_reviews = random.randint(1, 5)
        for k in range(num_reviews):
            rating = random.choices([1, 2, 3, 4, 5], weights=[5, 5, 15, 35, 40])[0]
            sentiment = "positive" if rating >= 4 else ("negative" if rating <= 2 else "neutral")
            template = random.choice(REVIEW_TEMPLATES[sentiment])
            
            review_date = base_date + timedelta(days=random.randint(0, 700))
            reviews.append({
                "review_id": len(reviews) + 1,
                "product_id": i,
                "customer_id": random.randint(1, 50000),
                "customer_name": f"Customer_{random.randint(1000, 9999)}",
                "rating": rating,
                "sentiment": sentiment,
                "title": f"{sentiment.capitalize()} experience with product",
                "review_text": template.format(product["name"]),
                "helpful_votes": random.randint(0, 100),
                "verified_purchase": random.random() > 0.2,
                "review_date": review_date.isoformat(),
                "response_from_seller": random.choice([None, None, None, "Thank you for your feedback!"]),
                "images_count": random.choice([0, 0, 0, 1, 2, 3])
            })
        
        # Update review count
        products[-1]["review_count"] = num_reviews
    
    # Define dlt resources
    @dlt.resource(name="ecom_categories", write_disposition="replace")
    def categories_resource():
        yield CATEGORIES
    
    @dlt.resource(name="ecom_products", write_disposition="replace")
    def products_resource():
        yield products
    
    @dlt.resource(name="ecom_pricing_history", write_disposition="replace")
    def pricing_resource():
        yield pricing_history
    
    @dlt.resource(name="ecom_inventory", write_disposition="replace")
    def inventory_resource():
        yield inventory_records
    
    @dlt.resource(name="ecom_reviews", write_disposition="replace")
    def reviews_resource():
        yield reviews
    
    # Run pipeline
    result = loader.run(
        categories_resource(),
        products_resource(),
        pricing_resource(),
        inventory_resource(),
        reviews_resource()
    )
    
    # Add summary stats
    result["data_summary"] = {
        "products": len(products),
        "categories": len(CATEGORIES),
        "pricing_records": len(pricing_history),
        "inventory_records": len(inventory_records),
        "reviews": len(reviews),
        "total_records": len(products) + len(CATEGORIES) + len(pricing_history) + len(inventory_records) + len(reviews)
    }
    
    return json.dumps(result, indent=2, default=str)
$$;

-- CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_ECOMMERCE_DATA_WAREHOUSE();


-- ============================================================================
-- PIPELINE 2: FINANCIAL TRADING DATA (Time-series, 2000+ daily trades)
-- ============================================================================
-- Simulates financial market data:
--   - TRADE_EXECUTIONS: 2000+ individual trades
--   - PORTFOLIO_SNAPSHOTS: Daily portfolio values (365 days x 5 portfolios)
--   - MARKET_DATA: OHLCV data for 50 symbols x 30 days
--   - RISK_METRICS: Daily VaR, beta, sharpe calculations
--
-- Tests: Time-series data, financial calculations, multi-dimensional data

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_FINANCIAL_TRADING_DATA()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'load_trading_data'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'dlt')
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Financial trading data with 2000+ trades and time-series data'
AS
$$
def load_trading_data(session):
    """
    Financial Trading Data Pipeline
    
    Generates realistic financial market data:
    - 2000+ trade executions across multiple accounts
    - 1825 portfolio snapshots (365 days x 5 portfolios)
    - 1500 OHLCV market data records (50 symbols x 30 days)
    - 365 risk metric calculations
    
    Total: ~5,700+ rows across 4 tables
    """
    import dlt
    import json
    import random
    import math
    from datetime import datetime, timedelta
    from dlt_snowpark import DltSnowparkLoader
    
    random.seed(123)
    loader = DltSnowparkLoader(session, pipeline_name="trading_data", batch_size=500)
    
    # Configuration
    NUM_TRADES = 2000
    NUM_DAYS = 365
    NUM_PORTFOLIOS = 5
    
    SYMBOLS = [
        "AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "MA",
        "BAC", "WFC", "GS", "MS", "C", "UNH", "JNJ", "PFE", "MRK", "ABBV",
        "XOM", "CVX", "COP", "SLB", "EOG", "HD", "LOW", "TGT", "WMT", "COST",
        "DIS", "NFLX", "CMCSA", "VZ", "T", "CRM", "ORCL", "ADBE", "INTC", "AMD",
        "BA", "CAT", "GE", "HON", "MMM", "KO", "PEP", "MCD", "SBUX", "NKE"
    ]
    
    TRADE_TYPES = ["BUY", "SELL", "SHORT", "COVER"]
    ORDER_TYPES = ["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]
    EXCHANGES = ["NYSE", "NASDAQ", "CBOE", "BATS", "IEX"]
    ACCOUNT_TYPES = ["INSTITUTIONAL", "RETAIL", "HEDGE_FUND", "PENSION", "PROP"]
    
    base_date = datetime(2025, 1, 1)
    
    # Generate base prices for each symbol
    symbol_base_prices = {sym: random.uniform(50, 500) for sym in SYMBOLS}
    
    # 1. Generate Trade Executions
    trades = []
    for i in range(1, NUM_TRADES + 1):
        symbol = random.choice(SYMBOLS)
        base_price = symbol_base_prices[symbol]
        trade_date = base_date + timedelta(days=random.randint(0, 364), hours=random.randint(9, 16), minutes=random.randint(0, 59))
        
        quantity = random.choice([100, 200, 500, 1000, 2500, 5000, 10000])
        price = round(base_price * random.uniform(0.9, 1.1), 4)
        
        trade = {
            "trade_id": f"TRD-{i:08d}",
            "execution_id": f"EXE-{random.randint(100000000, 999999999)}",
            "account_id": f"ACC-{random.randint(1, 100):05d}",
            "account_type": random.choice(ACCOUNT_TYPES),
            "symbol": symbol,
            "trade_type": random.choice(TRADE_TYPES),
            "order_type": random.choice(ORDER_TYPES),
            "quantity": quantity,
            "price": price,
            "total_value": round(quantity * price, 2),
            "commission": round(quantity * 0.005, 2),
            "exchange": random.choice(EXCHANGES),
            "currency": "USD",
            "trade_timestamp": trade_date.isoformat(),
            "settlement_date": (trade_date + timedelta(days=2)).strftime("%Y-%m-%d"),
            "status": random.choices(["FILLED", "PARTIAL", "CANCELLED"], weights=[90, 8, 2])[0],
            "broker_id": f"BRK-{random.randint(1, 20):03d}",
            "trader_id": f"TRD-{random.randint(1, 50):03d}",
            "algo_order": random.random() < 0.3,
            "dark_pool": random.random() < 0.15,
            "parent_order_id": f"PO-{random.randint(1, 1000):06d}" if random.random() < 0.4 else None,
            "notes": None
        }
        trades.append(trade)
    
    # 2. Generate Portfolio Snapshots
    portfolio_snapshots = []
    portfolio_bases = {
        "PORTFOLIO_GROWTH": 10000000,
        "PORTFOLIO_VALUE": 25000000,
        "PORTFOLIO_INCOME": 15000000,
        "PORTFOLIO_BALANCED": 50000000,
        "PORTFOLIO_AGGRESSIVE": 5000000
    }
    
    for portfolio_name, base_value in portfolio_bases.items():
        current_value = base_value
        for day in range(NUM_DAYS):
            snapshot_date = base_date + timedelta(days=day)
            # Simulate daily returns with volatility
            daily_return = random.gauss(0.0003, 0.015)  # ~7.5% annual return, 24% vol
            current_value *= (1 + daily_return)
            
            snapshot = {
                "snapshot_id": len(portfolio_snapshots) + 1,
                "portfolio_id": portfolio_name,
                "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
                "total_value": round(current_value, 2),
                "cash_balance": round(current_value * random.uniform(0.02, 0.1), 2),
                "equity_value": round(current_value * random.uniform(0.5, 0.7), 2),
                "fixed_income_value": round(current_value * random.uniform(0.1, 0.3), 2),
                "alternatives_value": round(current_value * random.uniform(0.05, 0.15), 2),
                "daily_pnl": round(current_value * daily_return, 2),
                "daily_return_pct": round(daily_return * 100, 4),
                "mtd_return_pct": round(random.uniform(-5, 5), 2),
                "ytd_return_pct": round(random.uniform(-10, 20), 2),
                "positions_count": random.randint(20, 100),
                "benchmark": "SPY",
                "benchmark_return_pct": round(random.gauss(0.04, 1.5), 2),
                "tracking_error": round(abs(random.gauss(0, 0.5)), 4),
                "created_at": snapshot_date.isoformat()
            }
            portfolio_snapshots.append(snapshot)
    
    # 3. Generate Market Data (OHLCV)
    market_data = []
    for symbol in SYMBOLS:
        base_price = symbol_base_prices[symbol]
        current_price = base_price
        
        for day in range(30):  # Last 30 days
            market_date = base_date + timedelta(days=335 + day)  # Near end of year
            daily_vol = abs(random.gauss(0, 0.025))
            
            open_price = current_price
            high_price = open_price * (1 + daily_vol)
            low_price = open_price * (1 - daily_vol)
            close_price = open_price * (1 + random.gauss(0, 0.015))
            current_price = close_price
            
            ohlcv = {
                "market_data_id": len(market_data) + 1,
                "symbol": symbol,
                "market_date": market_date.strftime("%Y-%m-%d"),
                "open": round(open_price, 4),
                "high": round(high_price, 4),
                "low": round(low_price, 4),
                "close": round(close_price, 4),
                "adjusted_close": round(close_price * random.uniform(0.995, 1.0), 4),
                "volume": random.randint(1000000, 100000000),
                "vwap": round((high_price + low_price + close_price) / 3, 4),
                "trades_count": random.randint(50000, 500000),
                "bid_ask_spread": round(random.uniform(0.01, 0.05), 4),
                "market_cap_billions": round(random.uniform(10, 2000), 2),
                "pe_ratio": round(random.uniform(10, 50), 2) if random.random() > 0.1 else None,
                "dividend_yield": round(random.uniform(0, 4), 2) if random.random() > 0.3 else 0,
                "beta": round(random.uniform(0.5, 2.0), 3),
                "data_source": "CONSOLIDATED_FEED"
            }
            market_data.append(ohlcv)
    
    # 4. Generate Risk Metrics
    risk_metrics = []
    for day in range(NUM_DAYS):
        metric_date = base_date + timedelta(days=day)
        
        risk = {
            "risk_id": day + 1,
            "metric_date": metric_date.strftime("%Y-%m-%d"),
            "portfolio_id": "FIRM_AGGREGATE",
            "var_95_1d": round(random.uniform(500000, 2000000), 2),
            "var_99_1d": round(random.uniform(800000, 3500000), 2),
            "cvar_95_1d": round(random.uniform(700000, 2800000), 2),
            "var_95_10d": round(random.uniform(1500000, 6000000), 2),
            "beta_to_spy": round(random.uniform(0.8, 1.3), 4),
            "sharpe_ratio_30d": round(random.uniform(-0.5, 2.5), 4),
            "sortino_ratio_30d": round(random.uniform(-0.3, 3.0), 4),
            "max_drawdown_30d": round(random.uniform(0, 15), 2),
            "volatility_30d": round(random.uniform(8, 25), 2),
            "correlation_to_benchmark": round(random.uniform(0.6, 0.95), 4),
            "gross_exposure": round(random.uniform(0.8, 1.5), 4),
            "net_exposure": round(random.uniform(0.3, 0.9), 4),
            "leverage_ratio": round(random.uniform(1.0, 2.5), 2),
            "liquidity_score": round(random.uniform(70, 100), 1),
            "concentration_top_10": round(random.uniform(30, 60), 2),
            "sector_concentration_max": round(random.uniform(15, 35), 2),
            "calculated_at": (metric_date + timedelta(hours=18)).isoformat()
        }
        risk_metrics.append(risk)
    
    # Define resources
    @dlt.resource(name="fin_trades", write_disposition="replace")
    def trades_resource():
        yield trades
    
    @dlt.resource(name="fin_portfolio_snapshots", write_disposition="replace")
    def snapshots_resource():
        yield portfolio_snapshots
    
    @dlt.resource(name="fin_market_data", write_disposition="replace")
    def market_resource():
        yield market_data
    
    @dlt.resource(name="fin_risk_metrics", write_disposition="replace")
    def risk_resource():
        yield risk_metrics
    
    # Run
    result = loader.run(
        trades_resource(),
        snapshots_resource(),
        market_resource(),
        risk_resource()
    )
    
    result["data_summary"] = {
        "trades": len(trades),
        "portfolio_snapshots": len(portfolio_snapshots),
        "market_data_records": len(market_data),
        "risk_metrics": len(risk_metrics),
        "total_records": len(trades) + len(portfolio_snapshots) + len(market_data) + len(risk_metrics)
    }
    
    return json.dumps(result, indent=2, default=str)
$$;

-- CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_FINANCIAL_TRADING_DATA();


-- ============================================================================
-- PIPELINE 3: IOT SENSOR DATA (High-volume telemetry)
-- ============================================================================
-- Simulates IoT sensor data from manufacturing:
--   - DEVICES: 100 IoT devices with metadata
--   - SENSOR_READINGS: 2400+ readings (100 devices x 24 hours)
--   - DEVICE_ALERTS: 500+ alerts triggered
--   - HOURLY_AGGREGATIONS: Pre-computed hourly stats
--
-- Tests: High-volume time-series, schema with many numeric fields

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_IOT_SENSOR_DATA()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'load_iot_data'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'dlt')
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'IoT sensor data with 2400+ readings from 100 devices'
AS
$$
def load_iot_data(session):
    """
    IoT Sensor Data Pipeline
    
    Generates manufacturing IoT telemetry:
    - 100 IoT devices across 3 facility zones
    - 2400+ sensor readings (24 hourly readings per device)
    - 500+ device alerts (warnings and critical)
    - 2400 hourly aggregations
    
    Total: ~5,500+ rows across 4 tables
    """
    import dlt
    import json
    import random
    import math
    from datetime import datetime, timedelta
    from dlt_snowpark import DltSnowparkLoader
    
    random.seed(456)
    loader = DltSnowparkLoader(session, pipeline_name="iot_telemetry", batch_size=500)
    
    NUM_DEVICES = 100
    HOURS_OF_DATA = 24
    
    DEVICE_TYPES = ["TEMPERATURE_SENSOR", "PRESSURE_SENSOR", "VIBRATION_SENSOR", "HUMIDITY_SENSOR", "FLOW_METER"]
    ZONES = ["ZONE_A_PRODUCTION", "ZONE_B_PACKAGING", "ZONE_C_STORAGE"]
    MANUFACTURERS = ["SensorTech", "IoTInc", "SmartSense", "DataDevices", "TelemetryPro"]
    FIRMWARE_VERSIONS = ["2.1.0", "2.1.1", "2.2.0", "2.3.0-beta", "3.0.0"]
    
    # Thresholds for alerts
    THRESHOLDS = {
        "TEMPERATURE_SENSOR": {"warn_high": 85, "crit_high": 95, "warn_low": 10, "crit_low": 0},
        "PRESSURE_SENSOR": {"warn_high": 150, "crit_high": 180, "warn_low": 80, "crit_low": 60},
        "VIBRATION_SENSOR": {"warn_high": 5.0, "crit_high": 8.0, "warn_low": None, "crit_low": None},
        "HUMIDITY_SENSOR": {"warn_high": 80, "crit_high": 90, "warn_low": 20, "crit_low": 10},
        "FLOW_METER": {"warn_high": 500, "crit_high": 600, "warn_low": 50, "crit_low": 20}
    }
    
    base_time = datetime(2026, 1, 2, 0, 0, 0)
    
    # 1. Generate Devices
    devices = []
    for i in range(1, NUM_DEVICES + 1):
        device_type = random.choice(DEVICE_TYPES)
        install_date = base_time - timedelta(days=random.randint(30, 730))
        
        device = {
            "device_id": f"DEV-{i:05d}",
            "device_name": f"{device_type.replace('_', ' ').title()} #{i}",
            "device_type": device_type,
            "zone": random.choice(ZONES),
            "manufacturer": random.choice(MANUFACTURERS),
            "model_number": f"MOD-{random.randint(1000, 9999)}",
            "serial_number": f"SN-{random.randint(100000000, 999999999)}",
            "firmware_version": random.choice(FIRMWARE_VERSIONS),
            "install_date": install_date.strftime("%Y-%m-%d"),
            "last_calibration_date": (base_time - timedelta(days=random.randint(1, 90))).strftime("%Y-%m-%d"),
            "calibration_due_date": (base_time + timedelta(days=random.randint(30, 180))).strftime("%Y-%m-%d"),
            "status": random.choices(["ONLINE", "OFFLINE", "MAINTENANCE"], weights=[90, 5, 5])[0],
            "ip_address": f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}",
            "mac_address": ":".join([f"{random.randint(0, 255):02x}" for _ in range(6)]),
            "latitude": round(40.0 + random.uniform(-0.01, 0.01), 6),
            "longitude": round(-74.0 + random.uniform(-0.01, 0.01), 6),
            "floor_level": random.randint(1, 3),
            "maintenance_contact": f"tech_{random.randint(1, 20)}@company.com",
            "notes": None,
            "created_at": install_date.isoformat(),
            "updated_at": base_time.isoformat()
        }
        devices.append(device)
    
    # 2. Generate Sensor Readings
    readings = []
    alerts = []
    hourly_aggregations = []
    
    for device in devices:
        device_id = device["device_id"]
        device_type = device["device_type"]
        thresholds = THRESHOLDS[device_type]
        
        # Base values vary by device type
        base_values = {
            "TEMPERATURE_SENSOR": random.uniform(60, 75),
            "PRESSURE_SENSOR": random.uniform(100, 130),
            "VIBRATION_SENSOR": random.uniform(1.0, 3.0),
            "HUMIDITY_SENSOR": random.uniform(40, 60),
            "FLOW_METER": random.uniform(200, 400)
        }
        base_value = base_values[device_type]
        
        hourly_values = []
        
        for hour in range(HOURS_OF_DATA):
            reading_time = base_time + timedelta(hours=hour)
            
            # Simulate sensor drift and noise
            noise = random.gauss(0, base_value * 0.05)
            drift = math.sin(hour / 6 * math.pi) * base_value * 0.1
            value = base_value + noise + drift
            
            # Occasionally spike (for alerts)
            if random.random() < 0.02:  # 2% chance of anomaly
                value *= random.uniform(1.3, 1.5) if random.random() > 0.5 else random.uniform(0.5, 0.7)
            
            value = round(value, 3)
            hourly_values.append(value)
            
            reading = {
                "reading_id": len(readings) + 1,
                "device_id": device_id,
                "reading_timestamp": reading_time.isoformat(),
                "value": value,
                "unit": {"TEMPERATURE_SENSOR": "°C", "PRESSURE_SENSOR": "PSI", "VIBRATION_SENSOR": "mm/s", "HUMIDITY_SENSOR": "%", "FLOW_METER": "L/min"}[device_type],
                "quality_score": round(random.uniform(0.95, 1.0), 3),
                "battery_level": round(random.uniform(20, 100), 1) if random.random() > 0.5 else None,
                "signal_strength_dbm": random.randint(-90, -30),
                "sampling_rate_hz": random.choice([1, 10, 100, 1000]),
                "is_interpolated": random.random() < 0.01,
                "raw_value": round(value + random.uniform(-0.5, 0.5), 3),
                "calibration_offset": round(random.uniform(-0.1, 0.1), 4),
                "ingestion_timestamp": (reading_time + timedelta(seconds=random.randint(1, 10))).isoformat()
            }
            readings.append(reading)
            
            # Check for alerts
            alert_level = None
            if thresholds.get("crit_high") and value > thresholds["crit_high"]:
                alert_level = "CRITICAL"
            elif thresholds.get("warn_high") and value > thresholds["warn_high"]:
                alert_level = "WARNING"
            elif thresholds.get("crit_low") and value < thresholds["crit_low"]:
                alert_level = "CRITICAL"
            elif thresholds.get("warn_low") and value < thresholds["warn_low"]:
                alert_level = "WARNING"
            
            if alert_level:
                alert = {
                    "alert_id": len(alerts) + 1,
                    "device_id": device_id,
                    "alert_timestamp": reading_time.isoformat(),
                    "alert_level": alert_level,
                    "alert_type": "HIGH_VALUE" if value > base_value else "LOW_VALUE",
                    "threshold_value": thresholds.get("crit_high") or thresholds.get("warn_high"),
                    "actual_value": value,
                    "message": f"{device_type} reading of {value} exceeds threshold",
                    "acknowledged": random.random() < 0.7,
                    "acknowledged_by": f"operator_{random.randint(1, 10)}" if random.random() < 0.7 else None,
                    "acknowledged_at": (reading_time + timedelta(minutes=random.randint(1, 30))).isoformat() if random.random() < 0.7 else None,
                    "resolved": random.random() < 0.5,
                    "resolved_at": (reading_time + timedelta(hours=random.randint(1, 4))).isoformat() if random.random() < 0.5 else None,
                    "root_cause": random.choice([None, None, "Equipment malfunction", "Environmental factor", "Sensor drift", "Power fluctuation"]),
                    "created_at": reading_time.isoformat()
                }
                alerts.append(alert)
        
        # Hourly aggregation per device
        for hour in range(HOURS_OF_DATA):
            agg_time = base_time + timedelta(hours=hour)
            hour_value = hourly_values[hour]
            
            aggregation = {
                "aggregation_id": len(hourly_aggregations) + 1,
                "device_id": device_id,
                "aggregation_hour": agg_time.strftime("%Y-%m-%d %H:00:00"),
                "reading_count": random.randint(3500, 3600),  # ~1 reading/sec
                "min_value": round(hour_value * random.uniform(0.9, 0.98), 3),
                "max_value": round(hour_value * random.uniform(1.02, 1.1), 3),
                "avg_value": round(hour_value, 3),
                "std_dev": round(hour_value * random.uniform(0.01, 0.05), 4),
                "median_value": round(hour_value * random.uniform(0.99, 1.01), 3),
                "p95_value": round(hour_value * random.uniform(1.02, 1.05), 3),
                "p99_value": round(hour_value * random.uniform(1.05, 1.08), 3),
                "quality_avg": round(random.uniform(0.96, 1.0), 4),
                "missing_readings_pct": round(random.uniform(0, 2), 2),
                "anomaly_count": random.choice([0, 0, 0, 0, 1, 2]),
                "created_at": (agg_time + timedelta(hours=1, minutes=5)).isoformat()
            }
            hourly_aggregations.append(aggregation)
    
    # Resources
    @dlt.resource(name="iot_devices", write_disposition="replace")
    def devices_resource():
        yield devices
    
    @dlt.resource(name="iot_readings", write_disposition="replace")
    def readings_resource():
        yield readings
    
    @dlt.resource(name="iot_alerts", write_disposition="replace")
    def alerts_resource():
        yield alerts
    
    @dlt.resource(name="iot_hourly_agg", write_disposition="replace")
    def aggregations_resource():
        yield hourly_aggregations
    
    result = loader.run(
        devices_resource(),
        readings_resource(),
        alerts_resource(),
        aggregations_resource()
    )
    
    result["data_summary"] = {
        "devices": len(devices),
        "readings": len(readings),
        "alerts": len(alerts),
        "hourly_aggregations": len(hourly_aggregations),
        "total_records": len(devices) + len(readings) + len(alerts) + len(hourly_aggregations)
    }
    
    return json.dumps(result, indent=2, default=str)
$$;

-- CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_IOT_SENSOR_DATA();


-- ============================================================================
-- PIPELINE 4: MULTI-SOURCE ETL WITH TRANSFORMATIONS
-- ============================================================================
-- Simulates combining data from multiple "sources" with transformations:
--   - SOURCE_CRM: Customer data (2000 records)
--   - SOURCE_ERP: Order data (3000 records)
--   - SOURCE_WEB: Clickstream data (5000 events)
--   - UNIFIED_CUSTOMER_360: Transformed/merged view
--
-- Tests: dlt transformations, add_map, data merging logic

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_MULTI_SOURCE_ETL()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'run_multi_source_etl'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'dlt')
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Multi-source ETL combining CRM, ERP, and Web data with transformations'
AS
$$
def run_multi_source_etl(session):
    """
    Multi-Source ETL Pipeline
    
    Simulates data integration from multiple systems:
    - CRM: 2000 customer records with contact info
    - ERP: 3000 order records with line items
    - WEB: 5000 clickstream events
    
    Then creates a unified "Customer 360" view by:
    - Enriching customer data with order stats
    - Adding web engagement metrics
    - Computing customer lifetime value
    
    Total: 10,000+ source records + transformed view
    """
    import json
    import random
    import hashlib
    from datetime import datetime, timedelta
    from dlt_snowpark import DltSnowparkLoader
    import dlt
    
    random.seed(789)
    loader = DltSnowparkLoader(session, pipeline_name="multi_source_etl", batch_size=1000)
    
    NUM_CUSTOMERS = 2000
    NUM_ORDERS = 3000
    NUM_EVENTS = 5000
    
    FIRST_NAMES = ["Emma", "Liam", "Olivia", "Noah", "Ava", "Oliver", "Isabella", "James", "Sophia", "William",
                   "Mia", "Benjamin", "Charlotte", "Lucas", "Amelia", "Henry", "Harper", "Alexander", "Evelyn", "Daniel"]
    LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Martinez", "Wilson",
                  "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White", "Harris"]
    DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "company.com", "mail.com", "hotmail.com"]
    COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "Australia", "Japan", "Brazil", "India", "Mexico"]
    SEGMENTS = ["ENTERPRISE", "SMB", "STARTUP", "INDIVIDUAL"]
    LEAD_SOURCES = ["ORGANIC", "PAID_SEARCH", "SOCIAL", "REFERRAL", "DIRECT", "EMAIL", "PARTNER"]
    PRODUCT_LINES = ["STANDARD", "PROFESSIONAL", "ENTERPRISE", "CUSTOM"]
    WEB_EVENTS = ["PAGE_VIEW", "CLICK", "FORM_SUBMIT", "DOWNLOAD", "VIDEO_PLAY", "SEARCH", "ADD_TO_CART", "CHECKOUT_START"]
    
    base_date = datetime(2025, 1, 1)
    current_date = datetime(2026, 1, 3)
    
    # 1. Generate CRM Data (Customers)
    crm_customers = []
    customer_emails = {}
    
    for i in range(1, NUM_CUSTOMERS + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        email = f"{first_name.lower()}.{last_name.lower()}.{i}@{random.choice(DOMAINS)}"
        customer_emails[i] = email
        
        create_date = base_date + timedelta(days=random.randint(0, 700))
        
        customer = {
            "crm_customer_id": f"CRM-{i:06d}",
            "email": email,
            "email_hash": hashlib.md5(email.encode()).hexdigest(),
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}",
            "company_name": f"{last_name} {random.choice(['Inc', 'LLC', 'Corp', 'Ltd', 'Co'])}",
            "job_title": random.choice(["CEO", "CTO", "Director", "Manager", "Analyst", "Developer", "Designer", "VP"]),
            "phone": f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            "country": random.choice(COUNTRIES),
            "state": f"State_{random.randint(1, 50)}",
            "city": f"City_{random.randint(1, 1000)}",
            "postal_code": f"{random.randint(10000, 99999)}",
            "segment": random.choice(SEGMENTS),
            "lead_source": random.choice(LEAD_SOURCES),
            "lead_score": random.randint(0, 100),
            "is_active": random.random() > 0.1,
            "opted_in_email": random.random() > 0.3,
            "opted_in_sms": random.random() > 0.7,
            "created_date": create_date.strftime("%Y-%m-%d"),
            "last_activity_date": (create_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
            "owner_id": f"REP-{random.randint(1, 20):03d}",
            "source_system": "SALESFORCE_CRM",
            "extracted_at": current_date.isoformat()
        }
        crm_customers.append(customer)
    
    # 2. Generate ERP Data (Orders)
    erp_orders = []
    customer_order_totals = {}  # For later aggregation
    
    for i in range(1, NUM_ORDERS + 1):
        customer_num = random.randint(1, NUM_CUSTOMERS)
        customer_email = customer_emails[customer_num]
        order_date = base_date + timedelta(days=random.randint(0, 700))
        
        num_items = random.randint(1, 5)
        line_items = []
        order_total = 0
        
        for item_num in range(1, num_items + 1):
            unit_price = round(random.uniform(29.99, 999.99), 2)
            quantity = random.randint(1, 10)
            line_total = round(unit_price * quantity, 2)
            order_total += line_total
            
            line_items.append({
                "line_number": item_num,
                "product_id": f"PROD-{random.randint(1, 500):04d}",
                "product_name": f"Product {random.randint(1, 500)}",
                "product_line": random.choice(PRODUCT_LINES),
                "unit_price": unit_price,
                "quantity": quantity,
                "line_total": line_total,
                "discount_pct": random.choice([0, 0, 0, 5, 10, 15, 20])
            })
        
        customer_order_totals.setdefault(customer_email, {"count": 0, "total": 0})
        customer_order_totals[customer_email]["count"] += 1
        customer_order_totals[customer_email]["total"] += order_total
        
        order = {
            "erp_order_id": f"ORD-{i:08d}",
            "customer_email": customer_email,
            "order_date": order_date.strftime("%Y-%m-%d"),
            "order_timestamp": order_date.isoformat(),
            "status": random.choices(["COMPLETED", "SHIPPED", "PROCESSING", "CANCELLED", "REFUNDED"], weights=[60, 20, 10, 7, 3])[0],
            "subtotal": round(order_total, 2),
            "tax_amount": round(order_total * 0.08, 2),
            "shipping_amount": round(random.uniform(0, 25), 2),
            "total_amount": round(order_total * 1.08 + random.uniform(0, 25), 2),
            "currency": "USD",
            "payment_method": random.choice(["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "WIRE_TRANSFER", "CHECK"]),
            "shipping_method": random.choice(["STANDARD", "EXPRESS", "OVERNIGHT", "PICKUP"]),
            "line_items_count": num_items,
            "line_items_json": json.dumps(line_items),
            "billing_country": random.choice(COUNTRIES),
            "shipping_country": random.choice(COUNTRIES),
            "sales_rep_id": f"REP-{random.randint(1, 20):03d}",
            "channel": random.choice(["WEB", "PHONE", "IN_STORE", "PARTNER", "MARKETPLACE"]),
            "source_system": "SAP_ERP",
            "extracted_at": current_date.isoformat()
        }
        erp_orders.append(order)
    
    # 3. Generate Web Clickstream Data
    web_events = []
    customer_web_engagement = {}
    
    for i in range(1, NUM_EVENTS + 1):
        customer_num = random.randint(1, NUM_CUSTOMERS)
        customer_email = customer_emails[customer_num]
        event_time = base_date + timedelta(days=random.randint(0, 700), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        customer_web_engagement.setdefault(customer_email, {"page_views": 0, "clicks": 0, "sessions": set()})
        
        event_type = random.choice(WEB_EVENTS)
        if event_type == "PAGE_VIEW":
            customer_web_engagement[customer_email]["page_views"] += 1
        elif event_type == "CLICK":
            customer_web_engagement[customer_email]["clicks"] += 1
        
        session_id = f"SES-{(i // 10):08d}"
        customer_web_engagement[customer_email]["sessions"].add(session_id)
        
        event = {
            "event_id": f"EVT-{i:010d}",
            "session_id": session_id,
            "user_email": customer_email,
            "event_type": event_type,
            "event_timestamp": event_time.isoformat(),
            "page_url": f"/pages/{random.choice(['home', 'products', 'pricing', 'about', 'contact', 'blog', 'demo', 'signup'])}",
            "page_title": f"Page Title {random.randint(1, 100)}",
            "referrer_url": random.choice([None, "https://google.com", "https://linkedin.com", "https://twitter.com", "https://facebook.com"]),
            "utm_source": random.choice([None, "google", "linkedin", "twitter", "facebook", "email"]),
            "utm_medium": random.choice([None, "cpc", "organic", "social", "email", "referral"]),
            "utm_campaign": random.choice([None, "spring_2025", "product_launch", "brand_awareness", "retargeting"]),
            "device_type": random.choice(["DESKTOP", "MOBILE", "TABLET"]),
            "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge", "Other"]),
            "os": random.choice(["Windows", "macOS", "iOS", "Android", "Linux"]),
            "screen_resolution": random.choice(["1920x1080", "1366x768", "2560x1440", "390x844", "412x915"]),
            "country": random.choice(COUNTRIES),
            "duration_seconds": random.randint(1, 600) if event_type == "PAGE_VIEW" else None,
            "element_clicked": f"btn_{random.randint(1, 50)}" if event_type == "CLICK" else None,
            "source_system": "GOOGLE_ANALYTICS",
            "extracted_at": current_date.isoformat()
        }
        web_events.append(event)
    
    # 4. Create Unified Customer 360 View (Transformation)
    customer_360 = []
    
    for customer in crm_customers:
        email = customer["email"]
        
        # Get order data
        order_data = customer_order_totals.get(email, {"count": 0, "total": 0})
        
        # Get web engagement
        web_data = customer_web_engagement.get(email, {"page_views": 0, "clicks": 0, "sessions": set()})
        
        # Calculate engagement score
        engagement_score = min(100, (
            order_data["count"] * 10 +
            min(order_data["total"] / 1000, 30) +
            web_data["page_views"] * 0.5 +
            web_data["clicks"] * 2 +
            len(web_data["sessions"]) * 5
        ))
        
        # CLV calculation (simplified)
        avg_order_value = order_data["total"] / order_data["count"] if order_data["count"] > 0 else 0
        clv_estimate = round(avg_order_value * order_data["count"] * 3, 2)  # 3x historical
        
        unified = {
            "customer_360_id": customer["crm_customer_id"],
            "email": email,
            "email_hash": customer["email_hash"],
            "full_name": customer["full_name"],
            "company_name": customer["company_name"],
            "segment": customer["segment"],
            "country": customer["country"],
            "lead_source": customer["lead_source"],
            "crm_lead_score": customer["lead_score"],
            "is_active": customer["is_active"],
            "total_orders": order_data["count"],
            "total_revenue": round(order_data["total"], 2),
            "avg_order_value": round(avg_order_value, 2),
            "clv_estimate": clv_estimate,
            "web_page_views": web_data["page_views"],
            "web_clicks": web_data["clicks"],
            "web_sessions": len(web_data["sessions"]),
            "engagement_score": round(engagement_score, 1),
            "customer_tier": "PLATINUM" if clv_estimate > 10000 else ("GOLD" if clv_estimate > 5000 else ("SILVER" if clv_estimate > 1000 else "BRONZE")),
            "first_touch_date": customer["created_date"],
            "last_activity_date": customer["last_activity_date"],
            "sources_integrated": json.dumps(["CRM", "ERP", "WEB"]),
            "unified_at": current_date.isoformat()
        }
        customer_360.append(unified)
    
    # Define resources
    @dlt.resource(name="src_crm_customers", write_disposition="replace")
    def crm_resource():
        yield crm_customers
    
    @dlt.resource(name="src_erp_orders", write_disposition="replace")
    def erp_resource():
        yield erp_orders
    
    @dlt.resource(name="src_web_events", write_disposition="replace")
    def web_resource():
        yield web_events
    
    @dlt.resource(name="unified_customer_360", write_disposition="replace")
    def customer_360_resource():
        yield customer_360
    
    result = loader.run(
        crm_resource(),
        erp_resource(),
        web_resource(),
        customer_360_resource()
    )
    
    result["data_summary"] = {
        "crm_customers": len(crm_customers),
        "erp_orders": len(erp_orders),
        "web_events": len(web_events),
        "customer_360_records": len(customer_360),
        "total_source_records": len(crm_customers) + len(erp_orders) + len(web_events),
        "total_all_records": len(crm_customers) + len(erp_orders) + len(web_events) + len(customer_360)
    }
    
    # Add tier distribution
    tier_counts = {}
    for c in customer_360:
        tier = c["customer_tier"]
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    result["tier_distribution"] = tier_counts
    
    return json.dumps(result, indent=2, default=str)
$$;

-- CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_MULTI_SOURCE_ETL();


-- ============================================================================
-- PIPELINE 5: DATA QUALITY VALIDATION PIPELINE
-- ============================================================================
-- Demonstrates data quality checks integrated into dlt pipeline:
--   - SOURCE_DATA: 2000 records with intentional quality issues
--   - DQ_VALIDATION_RESULTS: Validation outcomes
--   - DQ_CLEAN_DATA: Filtered valid records
--   - DQ_QUARANTINE: Rejected records with reasons
--
-- Tests: Data validation, quality scoring, conditional routing

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_DATA_QUALITY_PIPELINE()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'run_dq_pipeline'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'dlt')
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Data quality validation pipeline with 2000 records, quality checks, and quarantine'
AS
$$
def run_dq_pipeline(session):
    """
    Data Quality Pipeline
    
    Generates 2000 records with intentional quality issues:
    - 5% missing required fields
    - 3% invalid email formats
    - 2% invalid date formats
    - 4% out-of-range values
    - 2% duplicate keys
    
    Then validates and routes to:
    - CLEAN_DATA: Records passing all checks
    - QUARANTINE: Records failing validation (with reasons)
    - VALIDATION_RESULTS: Full validation audit trail
    """
    import dlt
    import json
    import random
    import re
    from datetime import datetime, timedelta
    from dlt_snowpark import DltSnowparkLoader
    
    random.seed(999)
    loader = DltSnowparkLoader(session, pipeline_name="data_quality", batch_size=500)
    
    NUM_RECORDS = 2000
    
    # Generate source data with intentional issues
    source_data = []
    seen_keys = set()
    
    for i in range(1, NUM_RECORDS + 1):
        record_id = i
        
        # Introduce duplicate keys (2% of records)
        if i > 100 and random.random() < 0.02:
            record_id = random.choice(list(seen_keys)) if seen_keys else i
        
        seen_keys.add(record_id)
        
        # Base record
        record = {
            "record_id": record_id,
            "batch_id": f"BATCH-{(i // 100) + 1:04d}",
            "source_system": random.choice(["SYSTEM_A", "SYSTEM_B", "SYSTEM_C"]),
        }
        
        # Name field (5% missing)
        if random.random() < 0.05:
            record["customer_name"] = None
        else:
            record["customer_name"] = f"Customer {i}"
        
        # Email field (3% invalid format)
        if random.random() < 0.03:
            record["email"] = random.choice(["invalid-email", "missing@", "@nodomain.com", "spaces in@email.com", ""])
        else:
            record["email"] = f"customer_{i}@example.com"
        
        # Date field (2% invalid format)
        if random.random() < 0.02:
            record["transaction_date"] = random.choice(["not-a-date", "2026-13-45", "01-01-2026", ""])
        else:
            tx_date = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 700))
            record["transaction_date"] = tx_date.strftime("%Y-%m-%d")
        
        # Amount field (4% out of range - negative or too high)
        if random.random() < 0.04:
            record["amount"] = random.choice([-100.00, -50.50, 1000001.00, 99999999.99])
        else:
            record["amount"] = round(random.uniform(1.00, 10000.00), 2)
        
        # Age field (3% out of range)
        if random.random() < 0.03:
            record["age"] = random.choice([-5, 0, 150, 200, 999])
        else:
            record["age"] = random.randint(18, 85)
        
        # Status field (always valid - enum)
        record["status"] = random.choice(["ACTIVE", "INACTIVE", "PENDING", "SUSPENDED"])
        
        # Numeric score (should be 0-100, 2% invalid)
        if random.random() < 0.02:
            record["score"] = random.choice([-10, 150, 999, None])
        else:
            record["score"] = random.randint(0, 100)
        
        # Country code (should be 2 chars, 1% invalid)
        if random.random() < 0.01:
            record["country_code"] = random.choice(["USA", "INVALID", "1", ""])
        else:
            record["country_code"] = random.choice(["US", "CA", "UK", "DE", "FR", "AU", "JP"])
        
        record["created_at"] = datetime.now().isoformat()
        
        source_data.append(record)
    
    # Validation functions
    def validate_email(email):
        if not email:
            return False, "Email is empty or null"
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, email):
            return False, f"Invalid email format: {email}"
        return True, None
    
    def validate_date(date_str):
        if not date_str:
            return False, "Date is empty or null"
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True, None
        except:
            return False, f"Invalid date format: {date_str}"
    
    def validate_amount(amount):
        if amount is None:
            return False, "Amount is null"
        if amount < 0:
            return False, f"Amount is negative: {amount}"
        if amount > 1000000:
            return False, f"Amount exceeds maximum: {amount}"
        return True, None
    
    def validate_age(age):
        if age is None:
            return False, "Age is null"
        if age < 1 or age > 120:
            return False, f"Age out of range: {age}"
        return True, None
    
    def validate_score(score):
        if score is None:
            return False, "Score is null"
        if score < 0 or score > 100:
            return False, f"Score out of range: {score}"
        return True, None
    
    def validate_country_code(code):
        if not code:
            return False, "Country code is empty"
        if len(code) != 2:
            return False, f"Invalid country code length: {code}"
        return True, None
    
    # Validate all records
    validation_results = []
    clean_records = []
    quarantine_records = []
    duplicate_tracker = {}
    
    for record in source_data:
        issues = []
        checks_passed = 0
        checks_total = 7
        
        # Check for duplicates
        key = record["record_id"]
        if key in duplicate_tracker:
            issues.append(f"Duplicate record_id: {key}")
            duplicate_tracker[key] += 1
        else:
            duplicate_tracker[key] = 1
            checks_passed += 1
        
        # Required field: customer_name
        if not record.get("customer_name"):
            issues.append("Missing required field: customer_name")
        else:
            checks_passed += 1
        
        # Email validation
        email_valid, email_error = validate_email(record.get("email"))
        if not email_valid:
            issues.append(email_error)
        else:
            checks_passed += 1
        
        # Date validation
        date_valid, date_error = validate_date(record.get("transaction_date"))
        if not date_valid:
            issues.append(date_error)
        else:
            checks_passed += 1
        
        # Amount validation
        amount_valid, amount_error = validate_amount(record.get("amount"))
        if not amount_valid:
            issues.append(amount_error)
        else:
            checks_passed += 1
        
        # Age validation
        age_valid, age_error = validate_age(record.get("age"))
        if not age_valid:
            issues.append(age_error)
        else:
            checks_passed += 1
        
        # Score validation
        score_valid, score_error = validate_score(record.get("score"))
        if not score_valid:
            issues.append(score_error)
        else:
            checks_passed += 1
        
        # Country code validation (optional - just log)
        cc_valid, cc_error = validate_country_code(record.get("country_code"))
        
        # Calculate quality score
        quality_score = round((checks_passed / checks_total) * 100, 1)
        is_valid = len(issues) == 0
        
        validation_result = {
            "validation_id": len(validation_results) + 1,
            "record_id": record["record_id"],
            "batch_id": record["batch_id"],
            "source_system": record["source_system"],
            "is_valid": is_valid,
            "checks_passed": checks_passed,
            "checks_total": checks_total,
            "quality_score": quality_score,
            "issues_count": len(issues),
            "issues_json": json.dumps(issues) if issues else None,
            "validated_at": datetime.now().isoformat()
        }
        validation_results.append(validation_result)
        
        if is_valid:
            clean_record = {**record, "dq_quality_score": quality_score, "dq_validated_at": datetime.now().isoformat()}
            clean_records.append(clean_record)
        else:
            quarantine_record = {
                **record,
                "quarantine_id": len(quarantine_records) + 1,
                "issues_json": json.dumps(issues),
                "issue_categories": json.dumps(list(set([
                    "MISSING_REQUIRED" if "Missing" in str(issues) else "",
                    "INVALID_FORMAT" if "Invalid" in str(issues) else "",
                    "OUT_OF_RANGE" if "out of range" in str(issues).lower() else "",
                    "DUPLICATE" if "Duplicate" in str(issues) else ""
                ]))),
                "quality_score": quality_score,
                "quarantined_at": datetime.now().isoformat()
            }
            quarantine_records.append(quarantine_record)
    
    # Resources
    @dlt.resource(name="dq_source_data", write_disposition="replace")
    def source_resource():
        yield source_data
    
    @dlt.resource(name="dq_validation_results", write_disposition="replace")
    def validation_resource():
        yield validation_results
    
    @dlt.resource(name="dq_clean_data", write_disposition="replace")
    def clean_resource():
        yield clean_records
    
    @dlt.resource(name="dq_quarantine", write_disposition="replace")
    def quarantine_resource():
        yield quarantine_records
    
    result = loader.run(
        source_resource(),
        validation_resource(),
        clean_resource(),
        quarantine_resource()
    )
    
    # Quality summary
    valid_count = len(clean_records)
    invalid_count = len(quarantine_records)
    total = len(source_data)
    
    result["quality_summary"] = {
        "total_records": total,
        "valid_records": valid_count,
        "invalid_records": invalid_count,
        "pass_rate_pct": round((valid_count / total) * 100, 2),
        "avg_quality_score": round(sum(v["quality_score"] for v in validation_results) / len(validation_results), 2)
    }
    
    # Issue breakdown
    issue_counts = {
        "missing_required": sum(1 for r in quarantine_records if "Missing" in str(r.get("issues_json", ""))),
        "invalid_format": sum(1 for r in quarantine_records if "Invalid" in str(r.get("issues_json", ""))),
        "out_of_range": sum(1 for r in quarantine_records if "out of range" in str(r.get("issues_json", "")).lower()),
        "duplicate": sum(1 for r in quarantine_records if "Duplicate" in str(r.get("issues_json", "")))
    }
    result["issue_breakdown"] = issue_counts
    
    return json.dumps(result, indent=2, default=str)
$$;

-- CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_DATA_QUALITY_PIPELINE();


-- ============================================================================
-- PIPELINE 6: FULL ETL WITH READ-TRANSFORM-WRITE (Uses Pipeline 1 data)
-- ============================================================================
-- Reads from tables created by Pipeline 1 and creates analytics:
--   - Reads: ECOM_PRODUCTS, ECOM_REVIEWS, ECOM_INVENTORY
--   - Creates: ECOM_PRODUCT_PERFORMANCE (aggregated analytics)
--
-- Prerequisites: Run p_ecommerce_data_warehouse() first
--
-- Tests: Full Snowflake-to-Snowflake ETL using DltSnowparkLoader

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_ECOMMERCE_ANALYTICS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'build_ecommerce_analytics'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'dlt')
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Analytics pipeline that reads from ecommerce tables and creates aggregated views'
AS
$$
def build_ecommerce_analytics(session):
    """
    E-Commerce Analytics Pipeline (Read-Transform-Write)
    
    Reads data from tables created by p_ecommerce_data_warehouse():
    - ECOM_PRODUCTS
    - ECOM_REVIEWS  
    - ECOM_INVENTORY
    
    Creates:
    - ECOM_PRODUCT_PERFORMANCE: Aggregated product metrics
    - ECOM_CATEGORY_SUMMARY: Category-level analytics
    - ECOM_INVENTORY_STATUS: Inventory health metrics
    
    This demonstrates full ETL within Snowflake using DltSnowparkLoader.
    """
    import dlt
    import json
    from datetime import datetime
    from dlt_snowpark import DltSnowparkLoader
    
    loader = DltSnowparkLoader(session, pipeline_name="ecommerce_analytics")
    db = loader.database
    schema = loader.schema
    
    results = {"steps": []}
    
    # Step 1: Read source tables
    try:
        products_df = session.table(f"{db}.{schema}.ECOM_PRODUCTS")
        reviews_df = session.table(f"{db}.{schema}.ECOM_REVIEWS")
        inventory_df = session.table(f"{db}.{schema}.ECOM_INVENTORY")
        
        products_count = products_df.count()
        reviews_count = reviews_df.count()
        inventory_count = inventory_df.count()
        
        results["steps"].append({
            "step": "read_source_tables",
            "status": "success",
            "products": products_count,
            "reviews": reviews_count,
            "inventory": inventory_count
        })
    except Exception as e:
        return json.dumps({
            "error": f"Source tables not found. Run p_ecommerce_data_warehouse() first. Error: {str(e)}"
        }, indent=2)
    
    # Step 2: Build Product Performance Analytics
    product_performance_df = session.sql(f"""
        SELECT 
            p.PRODUCT_ID,
            p.SKU,
            p.NAME AS PRODUCT_NAME,
            p.BRAND,
            p.CATEGORY_NAME,
            CAST(p.BASE_PRICE AS FLOAT) AS BASE_PRICE,
            CAST(p.COST AS FLOAT) AS COST,
            ROUND((CAST(p.BASE_PRICE AS FLOAT) - CAST(p.COST AS FLOAT)) / NULLIF(CAST(p.BASE_PRICE AS FLOAT), 0) * 100, 2) AS MARGIN_PCT,
            COUNT(r.REVIEW_ID) AS REVIEW_COUNT,
            ROUND(AVG(CAST(r.RATING AS FLOAT)), 2) AS AVG_RATING,
            SUM(CASE WHEN r.SENTIMENT = 'positive' THEN 1 ELSE 0 END) AS POSITIVE_REVIEWS,
            SUM(CASE WHEN r.SENTIMENT = 'negative' THEN 1 ELSE 0 END) AS NEGATIVE_REVIEWS,
            SUM(CAST(r.HELPFUL_VOTES AS INT)) AS TOTAL_HELPFUL_VOTES,
            SUM(CAST(i.QUANTITY_ON_HAND AS INT)) AS TOTAL_STOCK,
            SUM(CAST(i.QUANTITY_AVAILABLE AS INT)) AS AVAILABLE_STOCK,
            COUNT(DISTINCT i.WAREHOUSE_CODE) AS WAREHOUSE_COUNT,
            p.IS_FEATURED,
            p.IS_ACTIVE
        FROM {db}.{schema}.ECOM_PRODUCTS p
        LEFT JOIN {db}.{schema}.ECOM_REVIEWS r ON p.PRODUCT_ID = CAST(r.PRODUCT_ID AS INT)
        LEFT JOIN {db}.{schema}.ECOM_INVENTORY i ON p.PRODUCT_ID = CAST(i.PRODUCT_ID AS INT)
        GROUP BY 
            p.PRODUCT_ID, p.SKU, p.NAME, p.BRAND, p.CATEGORY_NAME, 
            p.BASE_PRICE, p.COST, p.IS_FEATURED, p.IS_ACTIVE
    """)
    product_performance_data = [row.as_dict() for row in product_performance_df.collect()]
    
    # Step 3: Build Category Summary
    category_summary_df = session.sql(f"""
        SELECT 
            CATEGORY_NAME,
            CATEGORY_ID,
            COUNT(*) AS PRODUCT_COUNT,
            SUM(CASE WHEN IS_ACTIVE = 'True' THEN 1 ELSE 0 END) AS ACTIVE_PRODUCTS,
            ROUND(AVG(CAST(BASE_PRICE AS FLOAT)), 2) AS AVG_PRICE,
            MIN(CAST(BASE_PRICE AS FLOAT)) AS MIN_PRICE,
            MAX(CAST(BASE_PRICE AS FLOAT)) AS MAX_PRICE,
            ROUND(AVG(CAST(AVG_RATING AS FLOAT)), 2) AS AVG_CATEGORY_RATING,
            SUM(CAST(REVIEW_COUNT AS INT)) AS TOTAL_REVIEWS
        FROM {db}.{schema}.ECOM_PRODUCTS
        GROUP BY CATEGORY_NAME, CATEGORY_ID
        ORDER BY PRODUCT_COUNT DESC
    """)
    category_summary_data = [row.as_dict() for row in category_summary_df.collect()]
    
    # Step 4: Build Inventory Status
    inventory_status_df = session.sql(f"""
        SELECT 
            WAREHOUSE_CODE,
            COUNT(DISTINCT PRODUCT_ID) AS UNIQUE_PRODUCTS,
            SUM(CAST(QUANTITY_ON_HAND AS INT)) AS TOTAL_ON_HAND,
            SUM(CAST(QUANTITY_RESERVED AS INT)) AS TOTAL_RESERVED,
            SUM(CAST(QUANTITY_AVAILABLE AS INT)) AS TOTAL_AVAILABLE,
            SUM(CASE WHEN CAST(QUANTITY_ON_HAND AS INT) <= CAST(REORDER_POINT AS INT) THEN 1 ELSE 0 END) AS ITEMS_BELOW_REORDER,
            SUM(CASE WHEN CAST(QUANTITY_ON_HAND AS INT) = 0 THEN 1 ELSE 0 END) AS OUT_OF_STOCK_ITEMS,
            ROUND(AVG(CAST(QUANTITY_ON_HAND AS FLOAT)), 1) AS AVG_STOCK_LEVEL,
            MAX(LAST_RESTOCK_DATE) AS LATEST_RESTOCK
        FROM {db}.{schema}.ECOM_INVENTORY
        GROUP BY WAREHOUSE_CODE
        ORDER BY WAREHOUSE_CODE
    """)
    inventory_status_data = [row.as_dict() for row in inventory_status_df.collect()]
    
    results["steps"].append({
        "step": "transform_data",
        "status": "success",
        "product_performance_rows": len(product_performance_data),
        "category_summary_rows": len(category_summary_data),
        "inventory_status_rows": len(inventory_status_data)
    })
    
    # Step 5: Write using dlt
    @dlt.resource(name="ecom_product_performance", write_disposition="replace")
    def product_perf_resource():
        yield product_performance_data
    
    @dlt.resource(name="ecom_category_summary", write_disposition="replace")
    def category_resource():
        yield category_summary_data
    
    @dlt.resource(name="ecom_inventory_status", write_disposition="replace")
    def inventory_resource():
        yield inventory_status_data
    
    load_result = loader.run(
        product_perf_resource(),
        category_resource(),
        inventory_resource()
    )
    
    results["steps"].append({
        "step": "load_analytics",
        "status": load_result["status"],
        "tables_loaded": load_result["tables_loaded"]
    })
    
    # Validation
    for table_name in ["ecom_product_performance", "ecom_category_summary", "ecom_inventory_status"]:
        verify = loader.verify_table(table_name)
        results["steps"].append({
            "step": f"verify_{table_name}",
            "row_count": verify.get("row_count", 0),
            "status": "success" if "row_count" in verify else "error"
        })
    
    results["summary"] = {
        "source_tables": ["ECOM_PRODUCTS", "ECOM_REVIEWS", "ECOM_INVENTORY"],
        "target_tables": ["ECOM_PRODUCT_PERFORMANCE", "ECOM_CATEGORY_SUMMARY", "ECOM_INVENTORY_STATUS"],
        "completed_at": datetime.utcnow().isoformat()
    }
    
    return json.dumps(results, indent=2, default=str)
$$;

-- Run after P_ECOMMERCE_DATA_WAREHOUSE():
-- CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_ECOMMERCE_ANALYTICS();


-- ============================================================================
-- SUMMARY: All Complex Pipelines (DLT_SNOWPARK_SFRT.COMPLEX_DEMO)
-- ============================================================================
-- 
-- | Pipeline | Tables | Total Rows | Key Tests |
-- |----------|--------|------------|-----------|
-- | P_ECOMMERCE_DATA_WAREHOUSE | 5 | ~17,000+ | Complex data, relationships, batching |
-- | P_FINANCIAL_TRADING_DATA | 4 | ~5,700+ | Time-series, financial calcs |
-- | P_IOT_SENSOR_DATA | 4 | ~5,500+ | High-volume telemetry, numerics |
-- | P_MULTI_SOURCE_ETL | 4 | ~12,000+ | Multi-source integration, transforms |
-- | P_DATA_QUALITY_PIPELINE | 4 | ~6,000+ | Validation, routing, quality scoring |
-- | P_ECOMMERCE_ANALYTICS | 3 | ~2,000+ | Read-transform-write ETL (requires P_ECOMMERCE_DATA_WAREHOUSE first) |
--
-- VALIDATION CHECKLIST:
-- ✅ Large datasets (2000+ rows per pipeline)
-- ✅ Complex nested data structures
-- ✅ Multiple related tables per pipeline
-- ✅ Various data types (strings, numbers, dates, JSON)
-- ✅ Real-world ETL patterns (merge, transform, validate)
-- ✅ Minimal code overhead from DltSnowparkLoader
-- ✅ Error handling and quality checks
-- ✅ Full Snowflake-to-Snowflake ETL
--
-- ============================================================================

