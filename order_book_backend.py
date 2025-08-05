import pandas as pd
from db_handler import init_db, compute_file_hash, is_file_already_processed, store_rows


def process_order_book(file_path, token_input, file_hash=None):
    try:
        df = pd.read_csv(file_path, header=None)
        df.columns = [
            'action', 'token_no', 'buy_order_no', 'sell_order_no', 'sequence_no',
            'epoch_time', 'buy_sell_flag', 'price', 'qty'
        ]
        df['buy_order_no'] = df['buy_order_no'].apply(lambda x: str(int(float(x))))
        df['sell_order_no'] = df['sell_order_no'].apply(lambda x: str(int(float(x))))
        df['token_no'] = df['token_no'].astype(str).str.strip()
        token_input = str(token_input).strip()
        df = df[df['token_no'] == token_input].copy()
        if df.empty:
            return "", "", f"Token number {token_input} not found in file or has no entries."
        df.sort_values('sequence_no', inplace=True)
        order_book = {}

        def process_N(row):
            key = (row['buy_order_no'], row['buy_sell_flag'])
            order_book[key] = {
                'price': row['price'],
                'qty': row['qty'],
                'buy_sell_flag': row['buy_sell_flag']
            }

        def process_M(row):
            key = (row['buy_order_no'], row['buy_sell_flag'])
            if key in order_book:
                order_book[key]['price'] = row['price']
                order_book[key]['qty'] = row['qty']

        def process_X(row):
            key = (row['buy_order_no'], row['buy_sell_flag'])
            if key in order_book:
                order_book[key]['qty'] -= row['qty']
                if order_book[key]['qty'] <= 0:
                    del order_book[key]

        def process_T(row):
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

        buy_orders = [v for v in order_book.values() if v['buy_sell_flag'] == 'B' and v['qty'] > 0]
        sell_orders = [v for v in order_book.values() if v['buy_sell_flag'] == 'S' and v['qty'] > 0]
        buy_df = pd.DataFrame(buy_orders)
        sell_df = pd.DataFrame(sell_orders)

        if buy_df.empty:
            buy_summary = pd.DataFrame(columns=['price', 'order_count', 'qty'])
        else:
            buy_summary = buy_df.groupby('price').agg(order_count=('qty', 'count'), qty=('qty', 'sum')).reset_index()

        if sell_df.empty:
            sell_summary = pd.DataFrame(columns=['price', 'order_count', 'qty'])
        else:
            sell_summary = sell_df.groupby('price').agg(order_count=('qty', 'count'), qty=('qty', 'sum')).reset_index()

        buy_summary = buy_summary.sort_values('price', ascending=False)
        sell_summary = sell_summary.sort_values('price', ascending=True)

        min_sell = sell_summary['price'].min() if not sell_summary.empty else float('inf')
        max_buy = buy_summary['price'].max() if not buy_summary.empty else float('-inf')
        buy_summary = buy_summary[buy_summary['price'] < min_sell]
        sell_summary = sell_summary[sell_summary['price'] > max_buy]

        # Return as pretty tables (string)
        buy_table = buy_summary.to_string(index=False)
        sell_table = sell_summary.to_string(index=False)

        structured_rows = df.to_dict(orient='records')  # Or however you want to structure it

        # Store rows if file_hash is provided
        if file_hash is not None:
            store_rows(file_hash, structured_rows)

        return buy_table, sell_table, None
    except Exception as e:
        return "", "", str(e)