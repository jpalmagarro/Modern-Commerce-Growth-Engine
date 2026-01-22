import duckdb
import pandas as pd
import numpy as np
from scipy import stats
from tabulate import tabulate
import os

DB_PATH = "mcge.duckdb"
EXPORT_DIR = "analytics_export"

def run_analysis():
    print("🚀 Starting Strategic Analysis...")
    con = duckdb.connect(DB_PATH)
    
    # 1. A/B Testing Significance
    print("\n--- 1. A/B Testing Analysis (Z-Test) ---")
    
    # We need to join sessions with AB groups.
    # Note: stg_web_events has 'customer_unique_id', and we have ab_test_groups.csv in raw.
    # Better to rely on dbt models. Let's assume we can fetch sessions and join with a random assignment query here 
    # OR better, if we had the ab_variant in fact_sessions. We didn't explicitly add it to fact_sessions dbt model.
    # Let's simple fetch from raw ab_test_groups and fact_sessions through SQL here.
    
    ab_query = """
    WITH ab_groups AS (
        SELECT * FROM read_csv_auto('data/raw/ab_test_groups.csv')
    ),
    sessions AS (
        SELECT user_id, is_converted FROM main_marts.fact_sessions
    ),
    joined AS (
        SELECT 
            g.ab_group,
            COUNT(*) as visitors,
            SUM(s.is_converted) as conversions
        FROM sessions s
        JOIN ab_groups g ON s.user_id = g.customer_unique_id
        GROUP BY 1
    )
    SELECT *, (conversions::FLOAT / visitors) as conversion_rate FROM joined
    """
    
    df_ab = con.sql(ab_query).df()
    print(tabulate(df_ab, headers='keys', tablefmt='psql'))
    
    # Calculate Statistical Significance
    if len(df_ab) == 2:
        group_a = df_ab.iloc[0]
        group_b = df_ab.iloc[1]
        
        # Two-proportion z-test
        # We can simulate samples
        
        # Standard Error
        p_pool = (group_a['conversions'] + group_b['conversions']) / (group_a['visitors'] + group_b['visitors'])
        se = np.sqrt(p_pool * (1 - p_pool) * (1/group_a['visitors'] + 1/group_b['visitors']))
        
        # Z-score
        diff = group_b['conversion_rate'] - group_a['conversion_rate']
        z_score = diff / se
        
        # P-value (Two-tailed)
        p_value = stats.norm.sf(abs(z_score)) * 2
        
        print(f"\nStats Results:")
        print(f"Difference: {diff*100:.2f}%")
        print(f"Z-Score: {z_score:.4f}")
        print(f"P-Value: {p_value:.4f}")
        
        if p_value < 0.05:
            print("✅ Result is Statistically Significant (p < 0.05)")
        else:
            print("❌ Result is NOT Statistically Significant")
            
    # 2. Financials (CAC)
    print("\n--- 2. Marketing Efficiency (CAC) ---")
    cac_query = """
    SELECT 
        channel,
        SUM(cost) as total_spend,
        SUM(attribution_conversions) as total_conversions,
        CASE WHEN SUM(attribution_conversions) > 0 
             THEN SUM(cost) / SUM(attribution_conversions) 
             ELSE 0 
        END as calculated_cac
    FROM main_marts.fact_daily_marketing
    GROUP BY 1
    ORDER BY calculated_cac ASC
    """
    df_cac = con.sql(cac_query).df()
    print(tabulate(df_cac, headers='keys', tablefmt='psql'))
    
    # 3. Export for Power BI
    print(f"\n--- 3. Exporting to {EXPORT_DIR}/ ---")
    os.makedirs(EXPORT_DIR, exist_ok=True)
    
    tables = ['fact_orders', 'fact_sessions', 'dim_customers', 'fact_daily_marketing']
    for t in tables:
        print(f"Exporting {t}...")
        con.sql(f"SELECT * FROM main_marts.{t}").write_csv(f"{EXPORT_DIR}/{t}.csv")
        
    print("✅ Export Complete.")

if __name__ == "__main__":
    run_analysis()
