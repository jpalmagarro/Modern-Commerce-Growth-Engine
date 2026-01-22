import pandas as pd
import numpy as np
import uuid
import random
from datetime import timedelta
from faker import Faker
import os

# Initialize Faker
fake = Faker()

# Configuration
DATA_DIR = "data/raw"
OUTPUT_DIR = "data/raw"
orders_file = os.path.join(DATA_DIR, "orders.csv")
customers_file = os.path.join(DATA_DIR, "customers.csv")

# Constants
SOURCES = ['organic_search', 'paid_search', 'social_facebook', 'social_instagram', 'direct', 'email', 'referral']
SOURCE_PROBS = [0.35, 0.25, 0.15, 0.10, 0.10, 0.04, 0.01]
DEVICES = ['mobile', 'desktop', 'tablet']
DEVICE_PROBS = [0.55, 0.40, 0.05]

def load_data():
    print("Loading data...")
    orders = pd.read_csv(orders_file)
    customers = pd.read_csv(customers_file)
    
    # Merge to get user_id (customer_unique_id)
    df = orders.merge(customers, on='customer_id', how='left')
    
    # Filter only delivered orders for buying sessions
    df = df[df['order_status'] == 'delivered'].copy()
    
    # Convert timestamps
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    
    return df

def generate_buying_sessions(df):
    print(f"Generating buying sessions for {len(df)} orders...")
    
    events = []
    
    # Pre-calculate random values for performance
    n = len(df)
    sources = np.random.choice(SOURCES, n, p=SOURCE_PROBS)
    devices = np.random.choice(DEVICES, n, p=DEVICE_PROBS)
    
    # This might take a while, but it's necessary for coherent sessions
    for i, (idx, row) in enumerate(df.iterrows()):
        purchase_time = row['order_purchase_timestamp']
        user_id = row['customer_unique_id']
        session_id = str(uuid.uuid4())
        source = sources[i]
        device = devices[i]
        
        # Determine session length (5 to 45 mins)
        duration_mins = random.randint(5, 45)
        start_time = purchase_time - timedelta(minutes=duration_mins)
        
        # 1. Landing (Page View)
        events.append({
            'event_id': str(uuid.uuid4()),
            'timestamp': start_time,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'page_view',
            'page_url': '/home',
            'source': source,
            'device': device
        })
        
        # 2. Browse Category
        t2 = start_time + timedelta(seconds=random.randint(30, 120))
        events.append({
            'event_id': str(uuid.uuid4()),
            'timestamp': t2,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'page_view',
            'page_url': f'/category/{fake.word()}',
            'source': source,
            'device': device
        })
        
        # 3. Product View
        t3 = t2 + timedelta(seconds=random.randint(60, 300))
        events.append({
            'event_id': str(uuid.uuid4()),
            'timestamp': t3,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'page_view',
            'page_url': f'/product/{row["order_id"]}', # Mocking product URL with order_id for trace
            'source': source,
            'device': device
        })
        
        # 4. Add to Cart
        t4 = t3 + timedelta(seconds=random.randint(20, 120))
        events.append({
            'event_id': str(uuid.uuid4()),
            'timestamp': t4,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'add_to_cart',
            'page_url': f'/product/{row["order_id"]}',
            'source': source,
            'device': device
        })
        
        # 5. Checkout Start
        t5 = t4 + timedelta(seconds=random.randint(30, 200))
        events.append({
            'event_id': str(uuid.uuid4()),
            'timestamp': t5,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'checkout_start',
            'page_url': '/checkout',
            'source': source,
            'device': device
        })
        
        # 6. Purchase (Order Placed) -> Matches Order Timestamp
        events.append({
            'event_id': str(uuid.uuid4()),
            'timestamp': purchase_time,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'order_placed',
            'page_url': '/checkout/success',
            'source': source,
            'device': device
        })
        
        if i % 5000 == 0:
            print(f"Processed {i} orders...")

    return pd.DataFrame(events)

def generate_ghost_sessions(df_buyers, multiplier=2):
    print("Generating ghost sessions (non-buyers)...")
    
    # We simulate traffic based on the date range of orders
    min_date = df_buyers['order_purchase_timestamp'].min()
    max_date = df_buyers['order_purchase_timestamp'].max()
    
    num_ghost_sessions = int(len(df_buyers) * multiplier)
    
    # Generate random timestamps
    date_range = (max_date - min_date).total_seconds()
    random_seconds = np.random.randint(0, int(date_range), num_ghost_sessions)
    timestamps = [min_date + timedelta(seconds=int(s)) for s in random_seconds]
    
    sources = np.random.choice(SOURCES, num_ghost_sessions, p=SOURCE_PROBS)
    devices = np.random.choice(DEVICES, num_ghost_sessions, p=DEVICE_PROBS)
    
    events = []
    
    for i in range(num_ghost_sessions):
        session_id = str(uuid.uuid4())
        # Ghost user (new random ID)
        user_id = str(uuid.uuid4()) 
        start_time = timestamps[i]
        source = sources[i]
        device = devices[i]
        
        # Short session: Home -> (Maybe Category) -> Bounce or Exit
        
        # 1. Page View
        events.append({
            'event_id': str(uuid.uuid4()),
            'timestamp': start_time,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'page_view',
            'page_url': '/home',
            'source': source,
            'device': device
        })
        
        # 50% chance to go further
        if random.random() > 0.5:
             t2 = start_time + timedelta(seconds=random.randint(10, 60))
             events.append({
                'event_id': str(uuid.uuid4()),
                'timestamp': t2,
                'user_id': user_id,
                'session_id': session_id,
                'event_type': 'page_view',
                'page_url': f'/category/{fake.word()}',
                'source': source,
                'device': device
            })
             
             # 20% of those view product
             if random.random() < 0.2:
                t3 = t2 + timedelta(seconds=random.randint(10, 60))
                events.append({
                    'event_id': str(uuid.uuid4()),
                    'timestamp': t3,
                    'user_id': user_id,
                    'session_id': session_id,
                    'event_type': 'page_view',
                    'page_url': '/product/random',
                    'source': source,
                    'device': device
                })
    
    return pd.DataFrame(events)

def generate_marketing_spend(all_events):
    print("Generating marketing spend...")
    # Aggregate by date and source
    all_events['date'] = all_events['timestamp'].dt.date
    
    # Count sessions (approx unique session_ids per source/date)
    daily_stats = all_events.groupby(['date', 'source'])['session_id'].nunique().reset_index()
    daily_stats.rename(columns={'session_id': 'sessions'}, inplace=True)
    
    # Assign CPC (Cost Per Click)
    # Social is cheaper, Paid Search is expensive
    cpc_map = {
        'organic_search': 0.0,
        'direct': 0.0,
        'email': 0.05,
        'referral': 0.0,
        'social_facebook': 0.60,
        'social_instagram': 0.75,
        'paid_search': 1.20
    }
    
    daily_stats['cost'] = daily_stats.apply(
        lambda x: x['sessions'] * cpc_map.get(x['source'], 0) * random.uniform(0.8, 1.2), axis=1
    )
    
    return daily_stats[['date', 'source', 'cost', 'sessions']]

def generate_ab_test_groups(df_orders):
    print("Generating A/B test groups...")
    # Get all unique users from the orders (and ghosts if possible, but let's stick to known users)
    # Actually, we should assign for ALL unique IDs in the Customers table to be comprehensive
    # But for this scope, let's use the ones we have in the orders df
    
    unique_ids = df_orders['customer_unique_id'].unique()
    
    groups = []
    for uid in unique_ids:
        # deterministic assignment based on hash
        h = hash(uid)
        group = 'test' if h % 2 == 0 else 'control'
        groups.append({'customer_unique_id': uid, 'ab_group': group})
        
    return pd.DataFrame(groups)

def main():
    # 1. Load Real Orders
    df_orders = load_data()
    # Sample for dev if needed (comment out for full run)
    # df_orders = df_orders.sample(5000) 
    
    # 2. Generate Buyers
    buyers_events = generate_buying_sessions(df_orders)
    
    # 3. Generate Non-Buyers
    ghost_events = generate_ghost_sessions(df_orders, multiplier=3)
    
    # 4. Combine
    all_events = pd.concat([buyers_events, ghost_events])
    all_events.sort_values('timestamp', inplace=True)
    
    # 5. Marketing Spend
    marketing_df = generate_marketing_spend(all_events)
    
    # 6. A/B Test Groups
    ab_df = generate_ab_test_groups(df_orders)
    
    # 7. Save
    print("Saving files...")
    all_events.drop(columns=['date'], errors='ignore').to_csv(os.path.join(OUTPUT_DIR, 'web_events.csv'), index=False)
    marketing_df.to_csv(os.path.join(OUTPUT_DIR, 'marketing_spend.csv'), index=False)
    ab_df.to_csv(os.path.join(OUTPUT_DIR, 'ab_test_groups.csv'), index=False)
    
    print("Done!")

if __name__ == "__main__":
    main()
