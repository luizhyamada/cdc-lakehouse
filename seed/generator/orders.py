import random
import time
from faker import Faker
from seed.db.connection import get_connection

class OrdersGenerator:
    def __init__(self):
        self.fake = Faker()

    def create_order(self):
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT customer_id
            FROM customers
            ORDER BY random()
            LIMIT 1
            """
        )

        customer = cursor.fetchone()

        cursor.execute(
            """
            SELECT product_id, price
            FROM products
            WHERE active = TRUE
            ORDER BY random()
            LIMIT 1
            """
        )

        product = cursor.fetchone()

        if not customer or not product:
            cursor.close()
            conn.close()
            return

        customer_id = customer[0]
        product_id = product[0]
        unit_price = float(product[1])

        quantity = random.randint(1, 5)
        amount = quantity * unit_price

        cursor.execute(
            """
            INSERT INTO orders (
                customer_id,
                product_id,
                quantity,
                unit_price,
                amount,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING order_id
            """,
            (
                customer_id,
                product_id,
                quantity,
                unit_price,
                amount,
                "PENDING"
            )
        )

        order_id = cursor.fetchone()[0]

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[INSERT] order_id={order_id}")

    def update_order(self):
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT order_id
            FROM orders
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return

        order_id = row[0]

        status = random.choice(
            [
                "PENDING",
                "PAID",
                "SHIPPED",
                "DELIVERED",
                "CANCELLED"
            ]
        )

        cursor.execute(
            """
            UPDATE orders
            SET
                status = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE order_id = %s
            """,
            (
                status,
                order_id
            )
        )

        conn.commit()
        cursor.close()
        conn.close()

        print(
            f"[UPDATE] order_id={order_id} status={status}"
        )

    def delete_order(self):
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT order_id
            FROM orders
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return

        order_id = row[0]

        cursor.execute(
            """
            DELETE FROM orders
            WHERE order_id = %s
            """,
            (order_id,)
        )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[DELETE] order_id={order_id}")

    def run(self):
        while True:
            operation = random.choices(
                population=["insert", "update", "delete"],
                weights=[80, 15, 5],
                k=1
            )[0]

            if operation == "insert":
                self.create_order()

            elif operation == "update":
                self.update_order()

            else:
                self.delete_order()

            time.sleep(2)