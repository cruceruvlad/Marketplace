"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2020
"""

from threading import Thread
from time import sleep


class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self)
        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time
        self.kwargs = kwargs

    def run(self):
        for cart in self.carts:
            self.id = self.marketplace.new_cart()
            for action in cart:
                if action['type'] == 'add':
                    ret = self.marketplace.add_to_cart(self.id, action)
                    while not ret:
                        sleep(self.retry_wait_time)
                        ret = self.marketplace.add_to_cart(self.id, action)
                else:
                    self.marketplace.remove_from_cart(self.id, action)
            ret = self.marketplace.place_order(self.id)
            for prod in ret:
                val = vars(prod['product'])
                if type(prod['product']).__name__ == 'Coffee':
                    print(self.kwargs['name'] + " bought " + type(prod['product']).__name__+
                          "(name='"+val['name']+"', price="+str(val['price'])+", acidity="+
                          str(val['acidity'])+", roast_level='"+val['roast_level']+"')")
                else:
                    print(self.kwargs['name'] + " bought " + type(prod['product']).__name__+
                          "(name='"+val['name']+"', price="+str(val['price'])+", type='"+
                          val['type']+"')")
