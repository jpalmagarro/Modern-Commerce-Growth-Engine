import pandas as pd
import os

DATA_DIR = "data/raw"

def verify_data():
    print("Loading data for verification...")
    orders = pd.read_csv(os.path.join(DATA_DIR, "orders.csv"))
    customers = pd.read_csv(os.path.join(DATA_DIR, "customers.csv"))
    web_events = pd.read_csv(os.path.join(DATA_DIR, "web_events.csv"))
    ab_groups = pd.read_csv(os.path.join(DATA_DIR, "ab_test_groups.csv"))
    
    print("\n--- 1. Volume Checks ---")
    print(f"Total Orders: {len(orders)}")
    print(f"Total Web Events: {len(web_events)}")
    print(f"Total A/B Assignments: {len(ab_groups)}")
    
    print("\n--- 2. Funnel Logic Check ---")
    event_counts = web_events['event_type'].value_counts()
    print("Event Distribution:")
    print(event_counts)
    
    home_views = len(web_events[web_events['page_url'] == '/home'])
    orders_placed = len(web_events[web_events['event_type'] == 'order_placed'])
    
    conversion_rate = (orders_placed / home_views) * 100 if home_views > 0 else 0
    print(f"\nEstimated Session Conversion Rate: {conversion_rate:.2f}% (Target: ~2-4%)")
    
    print("\n--- 3. Integrity Checks ---")
    # Check if buyers in web_events actually exist in orders
    buyers_in_events = web_events[web_events['event_type'] == 'order_placed']['user_id'].unique()
    
    # Get customer_unique_id for all delivered orders
    real_buyers = orders.merge(customers, on='customer_id')['customer_unique_id'].unique()
    
    # Intersection
    common_buyers = set(buyers_in_events).intersection(set(real_buyers))
    print(f"Unique Buyers in Web Events: {len(buyers_in_events)}")
    print(f"Real Unique Buyers (Delivered): {len(real_buyers)}")
    print(f"Matching Buyers: {len(common_buyers)}")
    
    if len(buyers_in_events) == len(common_buyers):
        print("SUCCESS: All simulated purchases link to real Olist users.")
    else:
        print(f"WARNING: {len(buyers_in_events) - len(common_buyers)} simulated buyers not found in real data.")

    print("\n--- 4. A/B Group Check ---")
    group_dist = ab_groups['ab_group'].value_counts(normalize=True)
    print("Group Allocation:")
    print(group_dist)
    if 0.48 < group_dist['control'] < 0.52:
        print("SUCCESS: Groups are balanced.")
    else:
        print("WARNING: Groups logic might be biased.")

if __name__ == "__main__":
    verify_data()
