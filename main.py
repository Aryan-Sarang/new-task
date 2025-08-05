import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
from order_book_backend import process_order_book
from db_handler import init_db, compute_file_hash, is_file_already_processed

class OrderBookApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Order Book Processor")
        self.root.geometry("700x500")

        self.file_path = None

        # --- File Selection ---
        self.file_label = tk.Label(root, text="File:")
        self.file_label.pack(pady=5)

        file_frame = tk.Frame(root)
        file_frame.pack()

        self.file_entry = tk.Entry(file_frame, width=60, state='readonly')
        self.file_entry.pack(side=tk.LEFT, padx=5)

        self.browse_button = tk.Button(file_frame, text="Browse", command=self.browse_file)
        self.browse_button.pack(side=tk.LEFT)

        # --- Token Input ---
        self.token_label = tk.Label(root, text="Token Number:")
        self.token_label.pack(pady=5)

        self.token_entry = tk.Entry(root)
        self.token_entry.pack()

        # --- Submit Button ---
        self.submit_button = tk.Button(root, text="Submit", command=self.process_input)
        self.submit_button.pack(pady=10)

        # --- Status Label ---
        self.status_label = tk.Label(root, text="Status: Waiting for input...", fg="blue")
        self.status_label.pack(pady=5)

        # --- Output Text Area ---
        self.output_text = tk.Text(root, height=15, width=80)
        self.output_text.pack(pady=10)

    def browse_file(self):
        path = filedialog.askopenfilename(
            filetypes=[("Text/Log Files", "*.txt;*.log"), ("All Files", "*.*")]
        )
        if path:
            try:
                file_hash = compute_file_hash(path)  # ✅ FIXED
                if is_file_already_processed(file_hash):
                    messagebox.showerror("Duplicate File", "This file has already been processed.")
                    return
                self.file_path = path
                self.file_hash = file_hash
                self.file_entry.config(state='normal')
                self.file_entry.delete(0, tk.END)
                self.file_entry.insert(0, path)
                self.file_entry.config(state='readonly')
                self.status_label.config(text="Status: File selected", fg="green")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to select file: {e}")


    def process_input(self):
        token = self.token_entry.get().strip()
        if not self.file_path:
            messagebox.showerror("Error", "Please select a file.")
            return
        if not token:
            messagebox.showerror("Error", "Please enter a token number.")
            return

        self.status_label.config(text="Status: Processing...", fg="orange")
        self.output_text.delete('1.0', tk.END)
        self.output_text.insert(tk.END, f"Processing file: {self.file_path}\n")
        self.output_text.insert(tk.END, f"Token: {token}\n")

        buy_table, sell_table, error = process_order_book(self.file_path, token, getattr(self, 'file_hash', None))
        if error:
            self.output_text.insert(tk.END, f"Error: {error}\n")
            self.status_label.config(text="Status: Error!", fg="red")
        elif not buy_table.strip() and not sell_table.strip():
            self.output_text.insert(tk.END, "No buy or sell orders found for this token.\n")
            self.status_label.config(text="Status: No Data", fg="orange")
        else:
            self.output_text.insert(tk.END, "\nBuy Orders Table:\n")
            self.output_text.insert(tk.END, buy_table + "\n")
            self.output_text.insert(tk.END, "\nSell Orders Table:\n")
            self.output_text.insert(tk.END, sell_table + "\n")
            self.status_label.config(text="Status: Done!", fg="green")


if __name__ == "__main__":
    init_db()  # Initialize DB and tables
    root = tk.Tk()
    app = OrderBookApp(root)
    root.mainloop()
