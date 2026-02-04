import os
import sqlite3
from typing import Any, Dict, List, Tuple

from flask import Flask, jsonify, request


DB_PATH = os.path.join(os.path.dirname(__file__), "ecommerce.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create schema and insert sample data if database is empty."""
    conn = get_connection()
    cur = conn.cursor()

    # Create tables (entities)
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS customer (
            id      INTEGER PRIMARY KEY,
            name    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS seller (
            id      INTEGER PRIMARY KEY,
            nation  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS catalog (
            id      INTEGER PRIMARY KEY,
            name    TEXT NOT NULL,
            color   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS "order" (
            id          INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            date        TEXT NOT NULL,
            item_id     INTEGER,
            FOREIGN KEY (customer_id) REFERENCES customer (id)
        );

        -- Relationship ORDER–CATALOG (contain) with quantity
        CREATE TABLE IF NOT EXISTS order_item (
            order_id   INTEGER NOT NULL,
            catalog_id INTEGER NOT NULL,
            quantity   INTEGER NOT NULL,
            PRIMARY KEY (order_id, catalog_id),
            FOREIGN KEY (order_id) REFERENCES "order" (id),
            FOREIGN KEY (catalog_id) REFERENCES catalog (id)
        );

        -- Relationship SELLER–CATALOG (deliver) with quantity
        CREATE TABLE IF NOT EXISTS delivery (
            seller_id  INTEGER NOT NULL,
            catalog_id INTEGER NOT NULL,
            quantity   INTEGER NOT NULL,
            PRIMARY KEY (seller_id, catalog_id),
            FOREIGN KEY (seller_id) REFERENCES seller (id),
            FOREIGN KEY (catalog_id) REFERENCES catalog (id)
        );
        """
    )

    # Ensure legacy databases have the new item_id column in "order"
    cur.execute('PRAGMA table_info("order")')
    columns = [row["name"] for row in cur.fetchall()]
    
    if "item_id" not in columns:
        cur.execute('ALTER TABLE "order" ADD COLUMN item_id INTEGER')

    # Insert sample data only if tables are empty
    def table_empty(table: str) -> bool:
        cur.execute(f"SELECT COUNT(*) AS c FROM {table}")
        return cur.fetchone()["c"] == 0

    if table_empty("customer"):
        cur.executemany(
            "INSERT INTO customer (id, name) VALUES (?, ?)",
            [(1, "Alice"), (2, "Bob"), (3, "Carol")],
        )

    if table_empty("seller"):
        cur.executemany(
            "INSERT INTO seller (id, nation) VALUES (?, ?)",
            [(1, "Italy"), (2, "France")],
        )

    if table_empty("catalog"):
        cur.executemany(
            "INSERT INTO catalog (id, name, color) VALUES (?, ?, ?)",
            [
                (1, "T-Shirt", "red"),
                (2, "T-Shirt", "blue"),
                (3, "Shoes", "black"),
            ],
        )

    if table_empty('"order"'):
        cur.executemany(
            'INSERT INTO "order" (id, customer_id, date, item_id) VALUES (?, ?, ?, ?)',
            [
                (1, 1, "2026-02-01", 1),
                (2, 2, "2026-02-02", 2),
            ],
        )

    if table_empty("order_item"):
        cur.executemany(
            "INSERT INTO order_item (order_id, catalog_id, quantity) VALUES (?, ?, ?)",
            [
                (1, 1, 2),
                (1, 3, 1),
                (2, 2, 1),
            ],
        )

    if table_empty("delivery"):
        cur.executemany(
            "INSERT INTO delivery (seller_id, catalog_id, quantity) VALUES (?, ?, ?)",
            [
                (1, 1, 100),
                (1, 2, 50),
                (2, 3, 70),
            ],
        )

    conn.commit()
    conn.close()


app = Flask(__name__)


def rows_to_list(rows: List[sqlite3.Row], column: str) -> List[Any]:
    return [row[column] for row in rows]


def get_all(table: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def get_by_id(table: str, id_value: Any) -> Dict[str, Any] | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE id = ?", (id_value,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_column_values(table: str, column: str) -> List[Any]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT {column} FROM {table}")
    rows = cur.fetchall()
    conn.close()
    return rows_to_list(rows, column)


def insert_row(table: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
    """Generic insert using keys from payload."""
    if not payload:
        return False, "Empty payload"

    columns = ", ".join(payload.keys())
    placeholders = ", ".join(["?"] * len(payload))
    values = list(payload.values())

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", values
        )
        conn.commit()
    except sqlite3.Error as exc:
        conn.rollback()
        conn.close()
        return False, str(exc)

    conn.close()
    return True, "inserted"


def insert_or_add_delivery(payload: Dict[str, Any]) -> Tuple[bool, str]:
    """Insert a new delivery or increase quantity if it already exists.

    This avoids UNIQUE constraint errors on (seller_id, catalog_id) while
    keeping a single row per seller+catalog pair.
    """
    required = {"seller_id", "catalog_id", "quantity"}
    if not required.issubset(payload):
        return False, "seller_id, catalog_id and quantity are required"

    seller_id = payload["seller_id"]
    catalog_id = payload["catalog_id"]
    quantity = payload["quantity"]

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT quantity FROM delivery WHERE seller_id = ? AND catalog_id = ?",
            (seller_id, catalog_id),
        )
        row = cur.fetchone()
        if row:
            # Increase existing quantity
            cur.execute(
                "UPDATE delivery SET quantity = quantity + ? "
                "WHERE seller_id = ? AND catalog_id = ?",
                (quantity, seller_id, catalog_id),
            )
        else:
            # Insert new row
            cur.execute(
                "INSERT INTO delivery (seller_id, catalog_id, quantity) "
                "VALUES (?, ?, ?)",
                (seller_id, catalog_id, quantity),
            )
        conn.commit()
    except sqlite3.Error as exc:
        conn.rollback()
        conn.close()
        return False, str(exc)

    conn.close()
    return True, "inserted"


# ---- CUSTOMER endpoints ----


@app.get("/customers")
def customers_all():
    return jsonify(get_all("customer"))


@app.get("/customers/<int:customer_id>")
def customers_by_id(customer_id: int):
    row = get_by_id("customer", customer_id)
    if row is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(row)


@app.get("/customers/ids")
def customers_ids():
    return jsonify(get_column_values("customer", "id"))


@app.get("/customers/names")
def customers_names():
    return jsonify(get_column_values("customer", "name"))


@app.put("/customers")
def customers_insert():
    ok, msg = insert_row("customer", request.json or {})
    status = 201 if ok else 400
    return jsonify({"success": ok, "message": msg}), status


# ---- SELLER endpoints ----


@app.get("/sellers")
def sellers_all():
    return jsonify(get_all("seller"))


@app.get("/sellers/<int:seller_id>")
def sellers_by_id(seller_id: int):
    row = get_by_id("seller", seller_id)
    if row is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(row)


@app.get("/sellers/ids")
def sellers_ids():
    return jsonify(get_column_values("seller", "id"))


@app.get("/sellers/nations")
def sellers_nations():
    return jsonify(get_column_values("seller", "nation"))


@app.put("/sellers")
def sellers_insert():
    ok, msg = insert_row("seller", request.json or {})
    status = 201 if ok else 400
    return jsonify({"success": ok, "message": msg}), status


# ---- CATALOG endpoints ----


@app.get("/catalog")
def catalog_all():
    return jsonify(get_all("catalog"))


@app.get("/catalog/<int:item_id>")
def catalog_by_id(item_id: int):
    row = get_by_id("catalog", item_id)
    if row is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(row)


@app.get("/catalog/ids")
def catalog_ids():
    return jsonify(get_column_values("catalog", "id"))


@app.get("/catalog/names")
def catalog_names():
    return jsonify(get_column_values("catalog", "name"))


@app.get("/catalog/colors")
def catalog_colors():
    return jsonify(get_column_values("catalog", "color"))


@app.put("/catalog")
def catalog_insert():
    ok, msg = insert_row("catalog", request.json or {})
    status = 201 if ok else 400
    return jsonify({"success": ok, "message": msg}), status


# ---- ORDER endpoints ----


@app.get("/orders")
def orders_all():
    return jsonify(get_all('"order"'))


@app.get("/orders/<int:order_id>")
def orders_by_id(order_id: int):
    row = get_by_id('"order"', order_id)
    if row is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(row)


@app.get("/orders/ids")
def orders_ids():
    return jsonify(get_column_values('"order"', "id"))


@app.get("/orders/dates")
def orders_dates():
    return jsonify(get_column_values('"order"', "date"))


@app.put("/orders")
def orders_insert():
    ok, msg = insert_row('"order"', request.json or {})
    status = 201 if ok else 400
    return jsonify({"success": ok, "message": msg}), status


# ---- ORDER_ITEM endpoints (relationship ORDER–CATALOG) ----


@app.get("/order-items")
def order_items_all():
    return jsonify(get_all("order_item"))


@app.get("/order-items/quantities")
def order_items_quantities():
    return jsonify(get_column_values("order_item", "quantity"))


@app.put("/order-items")
def order_items_insert():
    ok, msg = insert_row("order_item", request.json or {})
    status = 201 if ok else 400
    return jsonify({"success": ok, "message": msg}), status


# ---- DELIVERY endpoints (relationship SELLER–CATALOG) ----


@app.get("/deliveries")
def deliveries_all():
    return jsonify(get_all("delivery"))


@app.get("/deliveries/quantities")
def deliveries_quantities():
    return jsonify(get_column_values("delivery", "quantity"))


@app.put("/deliveries")
def deliveries_insert():
    ok, msg = insert_or_add_delivery(request.json or {})
    status = 201 if ok else 400
    return jsonify({"success": ok, "message": msg}), status


@app.get("/")
def root():
    """Simple health/info endpoint."""
    return jsonify({"service": "db-manager", "status": "ok"})


if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="0.0.0.0", port=5001)

