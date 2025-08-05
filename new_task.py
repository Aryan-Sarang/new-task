import pandas as pd

# Step 1: Load the hardcoded CSV file
file_path = r'C:\Users\Admin\Desktop\2_18_streamwise_raw_data.log'
df = pd.read_csv(file_path, header=None)
df.columns = [
    'action', 'token_no', 'buy_order_no', 'sell_order_no', 'sequence_no',
    'epoch_time', 'buy_sell_flag', 'price', 'qty'
]

# Clean up order numbers to remove .0 if present
df['buy_order_no'] = df['buy_order_no'].apply(lambda x: str(int(float(x))))
df['sell_order_no'] = df['sell_order_no'].apply(lambda x: str(int(float(x))))

# Strip spaces from token_no column first
df['token_no'] = df['token_no'].astype(str).str.strip()

print("Available token numbers:", df['token_no'].unique())

# Prompt for token number
token_input = input("Enter token number to process: ").strip()
if token_input not in df['token_no'].unique():
    print(f"Token number {token_input} not found in file.")
    exit(1)

# Filter for selected token
df = df[df['token_no'] == token_input].copy()

# Sort by sequence number
df.sort_values('sequence_no', inplace=True)

# Order book: dict of order_no -> order details
order_book = {}

def process_N(row):
    # Add new order
    key = (row['buy_order_no'], row['buy_sell_flag'])
    order_book[key] = {
        'price': row['price'],
        'qty': row['qty'],
        'buy_sell_flag': row['buy_sell_flag']
    }

def process_M(row):
    # Modify order
    key = (row['buy_order_no'], row['buy_sell_flag'])
    if key in order_book:
        order_book[key]['price'] = row['price']
        order_book[key]['qty'] = row['qty']

def process_X(row):
    # Cancel order
    key = (row['buy_order_no'], row['buy_sell_flag'])
    if key in order_book:
        order_book[key]['qty'] -= row['qty']
        if order_book[key]['qty'] <= 0:
            del order_book[key]

def process_T(row):
    # Trade: subtract qty from both buy and sell orders
    buy_key = (row['buy_order_no'], 'B')
    sell_key = (row['sell_order_no'], 'S')
    t_qty = row['qty']
    if buy_key in order_book:
        order_book[buy_key]['qty'] -= t_qty
        if order_book[buy_key]['qty'] <= 0:
            del order_book[buy_key]
    if sell_key in order_book:
        order_book[sell_key]['qty'] -= t_qty
        if order_book[sell_key]['qty'] <= 0:
            del order_book[sell_key]

# Process each row in order
for _, row in df.iterrows():
    flag = row['action']
    if flag == 'N':
        process_N(row)
    elif flag == 'M':
        process_M(row)
    elif flag == 'X':
        process_X(row)
    elif flag == 'T':
        process_T(row)

# Build buy and sell tables
buy_orders = [v for v in order_book.values() if v['buy_sell_flag'] == 'B' and v['qty'] > 0]
sell_orders = [v for v in order_book.values() if v['buy_sell_flag'] == 'S' and v['qty'] > 0]

buy_df = pd.DataFrame(buy_orders)
sell_df = pd.DataFrame(sell_orders)

if buy_df.empty:
    print("No buy orders found for this token.")
    buy_summary = pd.DataFrame(columns=['price', 'order_count', 'qty'])
else:
    buy_summary = buy_df.groupby('price').agg(order_count=('qty', 'count'), qty=('qty', 'sum')).reset_index()

if sell_df.empty:
    print("No sell orders found for this token.")
    sell_summary = pd.DataFrame(columns=['price', 'order_count', 'qty'])
else:
    sell_summary = sell_df.groupby('price').agg(order_count=('qty', 'count'), qty=('qty', 'sum')).reset_index()

# Sort tables
buy_summary = buy_summary.sort_values('price', ascending=False)
sell_summary = sell_summary.sort_values('price', ascending=True)

# Remove price overlaps
min_sell = sell_summary['price'].min() if not sell_summary.empty else float('inf')
max_buy = buy_summary['price'].max() if not buy_summary.empty else float('-inf')
buy_summary = buy_summary[buy_summary['price'] < min_sell]
sell_summary = sell_summary[sell_summary['price'] > max_buy]

# Export to Excel
output_file = 'order_book_summary2.xlsx'
with pd.ExcelWriter(output_file) as writer:
    buy_summary.to_excel(writer, sheet_name='BuyOrders', index=False)
    sell_summary.to_excel(writer, sheet_name='SellOrders', index=False)

print(f"Excel file '{output_file}' generated with Buy and Sell tables.")
print(df[df['action'] == 'N'])