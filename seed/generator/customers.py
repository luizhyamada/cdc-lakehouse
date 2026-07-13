import random
import time
from faker import Faker
from seed.db.connection import get_connection


class CustomerGenerator:
    """
        Data generator used to seed simulated customer traffic.

        This class continuously applies transactional mutations (CRUD operations) 
        directly onto a relational PostgreSQL database instance. It uses the `Faker` library 
        simulating realistic user behavior to trigger Change Data Capture (CDC) 
        engine downstream ingestion logs.
    """

    def __init__(self) -> None:
        """
            Initialize the generator utility.
        """
        self.fake = Faker()

    def create_customer(self) -> None:
        """
            Execute an INSERT statement to add a new customer data.

            Generates full user attributes (name, city, and email address), writes 
            them to the target database table, commits the transaction immediately, 
            and prints the returned primary key ID to standard output.

            Returns:
                None
        """
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO customers (
                name,
                city,
                email
            )
            VALUES (%s, %s, %s)
            RETURNING customer_id
            """,
            (
                self.fake.name(),
                self.fake.city(),
                self.fake.email()
            )
        )

        customer_id = cursor.fetchone()[0]

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[INSERT] customer_id={customer_id}")

    def update_customer(self) -> None:
        """
            Select a random existing customer and execute an UPDATE on their properties.

            Queries the target table for an arbitrary single record ID. If found, 
            mutates their city and email metrics with newly scrambled values, triggering 
            a CDC 'u' (Update) record type event.

            Returns:
                None
        """
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

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return

        customer_id = row[0]

        cursor.execute(
            """
            UPDATE customers
            SET
                city = %s,
                email = %s
            WHERE customer_id = %s
            """,
            (
                self.fake.city(),
                self.fake.email(),
                customer_id
            )
        )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[UPDATE] customer_id={customer_id}")

    def delete_customer(self) -> None:
        """
            Select a random existing customer and execute a hard DELETE.

            Queries the target table for an arbitrary single record ID. If found, 
            un-links and physically purges the profile row, prompting a CDC 'd' 
            (Delete) envelope downstream.

            Returns:
                None
        """
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

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return

        customer_id = row[0]

        cursor.execute(
            """
            DELETE FROM customers
            WHERE customer_id = %s
            """,
            (customer_id,)
        )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[DELETE] customer_id={customer_id}")

    def run(self) -> None:
        """
            Initiate the orchestration infinite loop for transaction generation.

            The execution loops indefinitely, using weighted probabilities to skew 
            the generation profile towards a production-like distribution:
                - Inserts: 70% probability
                - Updates: 20% probability
                - Deletes: 10% probability

            Returns:
                None
        """
        while True:
            operation = random.choices(
                population=["insert", "update", "delete"],
                weights=[70, 20, 10],
                k=1
            )[0]

            if operation == "insert":
                self.create_customer()

            elif operation == "update":
                self.update_customer()

            else:
                self.delete_customer()

            time.sleep(2)