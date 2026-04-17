import pandas as pd
import mysql.connector


# -------------------------------
# 1. Load Data
# -------------------------------
def load_data(file_path = "D:\\user_2\\trades(Sheet1).csv"):
    df = pd.read_csv(file_path)
    return df


# -------------------------------
# 2. Clean Data
# -------------------------------
def clean_data(df):
    df = df.dropna()

    df['quantity'] = df['quantity'].astype(int)
    df['price'] = df['price'].astype(float)
    df['side'] = df['side'].str.upper()

    return df


# -------------------------------
# 3. Connect to MySQL
# -------------------------------
def create_connection():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Asdf900@",   
        database="trades_db"
    )
    return conn


# -------------------------------
# 4. Create Table
# -------------------------------
def create_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        trade_id INT PRIMARY KEY,
        symbol VARCHAR(10),
        side VARCHAR(10),
        quantity INT,
        price FLOAT
    )
    """)


# -------------------------------
# 5. Insert Data
# -------------------------------
def insert_data(df, cursor, conn):
    query = """
    INSERT INTO trades (trade_id, symbol, side, quantity, price)
    VALUES (%s, %s, %s, %s, %s)
    """

    data = list(df.itertuples(index=False, name=None))
    cursor.executemany(query, data)

    conn.commit()


# -------------------------------
# 6. Calculate P&L
# -------------------------------
def calculate_pnl(df):
    buy_df = df[df['side'] == 'BUY']
    sell_df = df[df['side'] == 'SELL']

    merged = pd.merge(buy_df, sell_df, on='symbol', suffixes=('_buy', '_sell'))

    merged['pnl'] = (merged['price_sell'] - merged['price_buy']) * merged['quantity_buy']

    return merged[['symbol', 'pnl']]


# -------------------------------
# 7. Main Function
# -------------------------------
def main():
    # File path (IMPORTANT)
    file_path = "D:\\user_2\\trades(Sheet1).csv"

    # Step 1: Load
    df = load_data(file_path)

    # Step 2: Clean
    df = clean_data(df)

    # Step 3: Connect DB
    conn = create_connection()
    cursor = conn.cursor()

    # Step 4: Create table
    create_table(cursor)

    # Step 5: Insert data
    insert_data(df, cursor, conn)
    print("Data stored in MySQL ✅")

    # Step 6: Calculate P&L
    pnl = calculate_pnl(df)
    print("\nP&L:\n", pnl)

    # Close connection
    cursor.close()
    conn.close()


# Run program
if __name__ == "__main__":
    main()