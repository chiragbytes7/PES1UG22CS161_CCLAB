import json
import os
import sqlite3
from functools import wraps
from typing import List, Any, Callable


# Utility decorator for managing database connections
def with_connection(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = connect('carts.db')
        try:
            result = func(conn, *args, **kwargs)
        finally:
            conn.close()
        return result
    return wrapper


def connect(path: str) -> sqlite3.Connection:
    """Establish a connection to the SQLite database."""
    exists = os.path.exists(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    if not exists:
        create_tables(conn)
    return conn


def create_tables(conn: sqlite3.Connection) -> None:
    """Create the necessary database tables if they don't exist."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS carts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            contents TEXT DEFAULT '[]',
            cost REAL DEFAULT 0
        )
    ''')
    conn.commit()


@with_connection
def get_cart(conn: sqlite3.Connection, username: str) -> List[dict]:
    """Retrieve the cart for a given username."""
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM carts WHERE username = ?', (username,))
    cart = cursor.fetchone()
    if not cart:
        return []
    return json.loads(cart['contents'])


@with_connection
def add_to_cart(conn: sqlite3.Connection, username: str, product_id: int) -> None:
    """Add a product to the user's cart."""
    cursor = conn.cursor()
    cursor.execute('SELECT contents FROM carts WHERE username = ?', (username,))
    row = cursor.fetchone()
    contents = json.loads(row['contents']) if row else []
    contents.append(product_id)
    cursor.execute(
        'INSERT OR REPLACE INTO carts (username, contents, cost) VALUES (?, ?, ?)',
        (username, json.dumps(contents), 0)
    )
    conn.commit()


@with_connection
def remove_from_cart(conn: sqlite3.Connection, username: str, product_id: int) -> None:
    """Remove a product from the user's cart."""
    cursor = conn.cursor()
    cursor.execute('SELECT contents FROM carts WHERE username = ?', (username,))
    row = cursor.fetchone()
    if not row:
        return
    contents = json.loads(row['contents'])
    if product_id in contents:
        contents.remove(product_id)
        cursor.execute(
            'INSERT OR REPLACE INTO carts (username, contents, cost) VALUES (?, ?, ?)',
            (username, json.dumps(contents), 0)
        )
        conn.commit()


@with_connection
def delete_cart(conn: sqlite3.Connection, username: str) -> None:
    """Delete the user's cart."""
    cursor = conn.cursor()
    cursor.execute('DELETE FROM carts WHERE username = ?', (username,))
    conn.commit()
