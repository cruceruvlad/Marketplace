"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2020
"""
from threading import Lock

class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        self.total_per_producer = []
        self.total_per_product = []
        self.products = []
        self.carts = []
        self.producer_lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        id = len(self.total_per_producer)
        self.total_per_producer.append(0)
        return id


    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        self.producer_lock.acquire()
        if self.total_per_producer[producer_id] < self.queue_size_per_producer:
            self.total_per_producer[producer_id] += 1
            self.products.append({'product': product, 'producer': producer_id})
            rc = False
            for dic in self.total_per_product:
                if dic['product'] == product:
                    dic['total'] += 1
                    rc = True
                    break
            if not rc:
                self.total_per_product.append({'product': product, 'total': 1})
            self.producer_lock.release()
            return True
        self.producer_lock.release()
        return False

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        id = len(self.carts)
        self.carts.append([])
        return id

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        self.producer_lock.acquire()
        dic = next((dic for dic in self.total_per_product if dic["product"] == product['product']),
                    None)
        if dic is None:
            self.producer_lock.release()
            return False
        if product['quantity'] > dic['total']:
            self.producer_lock.release()
            return False
        dic['total'] -= product['quantity']
        while product['quantity'] > 0:
            item = next(item for item in self.products if item["product"] == product['product'])
            self.carts[cart_id].append(item)
            self.products.remove(item)
            product['quantity'] -= 1
        self.producer_lock.release()
        return True

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        self.producer_lock.acquire()
        dic = next((dic for dic in self.total_per_product if dic["product"] == product['product']))
        dic['total'] += product['quantity']

        while product['quantity'] > 0:
            item = next(item for item in self.carts[cart_id]
                        if item["product"] == product['product'])
            self.carts[cart_id].remove(item)
            self.products.append(item)
            product['quantity'] -= 1
        self.producer_lock.release()

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        self.producer_lock.acquire()
        for product in self.carts[cart_id]:
            self.total_per_producer[product['producer']] -= 1
        self.producer_lock.release()
        return self.carts[cart_id]
