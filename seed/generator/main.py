import time
from threading import Thread
from seed.generator.customers import CustomerGenerator
from seed.generator.orders import OrdersGenerator
from seed.generator.products import ProductsGenerator


def main():

    customer_generator = CustomerGenerator()
    product_generator = ProductsGenerator()
    order_generator = OrdersGenerator()

    customer_thread = Thread(
        target=customer_generator.run,
        daemon=True
    )

    product_thread = Thread(
        target=product_generator.run,
        daemon=True
    )

    order_thread = Thread(
        target=order_generator.run,
        daemon=True
    )

    customer_thread.start()
    product_thread.start()

    time.sleep(10)

    order_thread.start()

    customer_thread.join()
    product_thread.join()
    order_thread.join()


if __name__ == "__main__":
    main()