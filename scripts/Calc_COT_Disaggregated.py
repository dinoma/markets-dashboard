import pandas as pd
from sqlalchemy import create_engine, text
import os
from config import market_tickers, db_path_str

# Database URL from environment variable or config
db_url = os.environ.get(db_path_str)
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)

# Create the database engine
engine = create_engine(db_url)


def calculate_and_store_cot_data(market_name, data_type):
    print(f"Processing data for {market_name}...")

    # Determine the input and output table names
    input_table = f"{market_name.lower().replace(' ', '_')}_{data_type}"
    output_table = f"{market_name.lower().replace(' ', '_')}_{data_type}_calc"

    with engine.connect() as conn:
        # Fetch the data
        query = text(f"SELECT * FROM {input_table}")
        df = pd.read_sql(query, conn)

        # Ensure correct data types for calculations
        numeric_columns = {
            'cot_disaggregated_combined': [
                'open_interest_all', 'm_money_positions_long_all', 'm_money_positions_short_all',
                'prod_merc_positions_long', 'prod_merc_positions_short',
                'swap_positions_long_all', 'swap__positions_short_all'
            ],
            'cot_disaggregated_futures_only': [
                'open_interest_all', 'm_money_positions_long_all', 'm_money_positions_short_all',
                'prod_merc_positions_long', 'prod_merc_positions_short',
                'swap_positions_long_all', 'swap__positions_short_all'
            ]
        }

        for col in numeric_columns[data_type]:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Forward fill missing values
        df = df.ffill()

        # Calculate percentage changes
        df['pct_change_open_interest'] = df['open_interest_all'].pct_change() * 100
        df['pct_change_m_money_long'] = df['m_money_positions_long_all'].pct_change() * 100
        df['pct_change_m_money_short'] = df['m_money_positions_short_all'].pct_change() * 100
        df['pct_change_prod_merc_long'] = df['prod_merc_positions_long'].pct_change() * 100
        df['pct_change_prod_merc_short'] = df['prod_merc_positions_short'].pct_change() * 100
        df['pct_change_swap_long'] = df['swap_positions_long_all'].pct_change() * 100
        df['pct_change_swap_short'] = df['swap__positions_short_all'].pct_change() * 100

        # Calculate net positions
        df['m_money_net_positions'] = df['m_money_positions_long_all'] - df['m_money_positions_short_all']
        df['prod_merc_net_positions'] = df['prod_merc_positions_long'] - df['prod_merc_positions_short']
        df['swap_net_positions'] = df['swap_positions_long_all'] - df['swap__positions_short_all']

        # Calculate percentage change in net positions
        df['pct_change_m_money_net_positions'] = df['m_money_net_positions'].pct_change() * 100
        df['pct_change_prod_merc_net_positions'] = df['prod_merc_net_positions'].pct_change() * 100
        df['pct_change_swap_net_positions'] = df['swap_net_positions'].pct_change() * 100

        # Calculate 26-week indices
        def calculate_index(series):
            min_val = series.rolling(window=26, min_periods=1).min()
            max_val = series.rolling(window=26, min_periods=1).max()
            return ((series - min_val) / (max_val - min_val)) * 100

        df['m_money_26w_index'] = calculate_index(df['m_money_net_positions'])
        df['prod_merc_26w_index'] = calculate_index(df['prod_merc_net_positions'])
        df['swap_26w_index'] = calculate_index(df['swap_net_positions'])

        # Select only necessary columns for the new table
        calc_df = df[['report_date_as_yyyy_mm_dd', 'pct_change_open_interest',
                      'pct_change_m_money_long', 'pct_change_m_money_short',
                      'pct_change_prod_merc_long', 'pct_change_prod_merc_short',
                      'pct_change_swap_long', 'pct_change_swap_short',
                      'm_money_net_positions', 'prod_merc_net_positions', 'swap_net_positions',
                      'pct_change_m_money_net_positions', 'pct_change_prod_merc_net_positions',
                      'pct_change_swap_net_positions',
                      'm_money_26w_index', 'prod_merc_26w_index', 'swap_26w_index']]

        # Round numbers to 1 decimal place
        calc_df = calc_df.round(1)

        # Save to new table, appending instead of replacing
        try:
            calc_df.to_sql(output_table, engine, if_exists='append', index=False, method="multi")
            print(f"Calculated data for {market_name} saved to {output_table}.")
        except Exception as e:
            print(f"Error saving data for {market_name}: {e}")


# Calculate and store data for all markets and types
data_types = ['cot_disaggregated_combined', 'cot_disaggregated_futures_only']

for market in market_tickers.keys():
    for data_type in data_types:
        calculate_and_store_cot_data(market, data_type)
