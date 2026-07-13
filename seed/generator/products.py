import random
import time
from faker import Faker
from seed.db.connection import get_connection

class ProductsGenerator:
    def __init__(self):
        self.fake = Faker()
        self.categories = [
            "Electronics",
            "Books",
            "Sports",
            "Home",
            "Fashion",
            "Gaming",
            "Food"
        ]
        self.brands = [
            "Apple",
            "Samsung",
            "Sony",
            "Dell",
            "Nike",
            "Adidas",
            "Logitech",
            "Keychron",
            "LG"
        ]

    def create_product(self):
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO products (
                product_name,
                category,
                brand,
                price,
                stock_quantity,
                active
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING product_id
            """,
            (
                self.fake.word().title(),
                random.choice(self.categories),
                random.choice(self.brands),
                round(random.uniform(10, 5000), 2),
                random.randint(0, 500),
                True
            )
        )

        product_id = cursor.fetchone()[0]

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[INSERT] product_id={product_id}")

    def update_product(self):
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT product_id
            FROM products
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return

        product_id = row[0]

        operation = random.choice(
            [
                "price",
                "stock",
                "active"
            ]
        )

        if operation == "price":

            cursor.execute(
                """
                UPDATE products
                SET
                    price = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_id = %s
                """,
                (
                    round(random.uniform(10, 5000), 2),
                    product_id
                )
            )

        elif operation == "stock":

            cursor.execute(
                """
                UPDATE products
                SET
                    stock_quantity = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_id = %s
                """,
                (
                    random.randint(0, 500),
                    product_id
                )
            )

        else:

            cursor.execute(
                """
                UPDATE products
                SET
                    active = NOT active,
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_id = %s
                """,
                (product_id,)
            )

        conn.commit()
        cursor.close()
        conn.close()

        print(
            f"[UPDATE] product_id={product_id} operation={operation}"
        )

    def delete_product(self):
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT product_id
            FROM products
            ORDER BY random()
            LIMIT 1
            """
        )

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return

        product_id = row[0]

        cursor.execute(
            """
            DELETE FROM products
            WHERE product_id = %s
            """,
            (product_id,)
        )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[DELETE] product_id={product_id}")

    def run(self):
        while True:
            operation = random.choices(
                population=["insert", "update", "delete"],
                weights=[70, 20, 10],
                k=1
            )[0]

            if operation == "insert":
                self.create_product()

            elif operation == "update":
                self.update_product()

            else:
                self.delete_product()

            time.sleep(2)