import unittest
from fangde.orders import OrderAutomation

class PriceTests(unittest.TestCase):
    def test_order_prices_round_directionally_without_float_arithmetic(self):
        for price, buy, sell in (
            ("2.3625", "2.37", "2.36"),
            ("2.3600", "2.36", "2.36"),
            ("2.36", "2.36", "2.36"),
            ("2.3", "2.3", "2.3"),
            ("2", "2", "2"),
            ("9.999", "10.00", "9.99"),
            ("0.010001", "0.02", "0.01"),
            ("2.360000000000000000000000000001", "2.37", "2.36"),
            ("23625e-4", "2.37", "2.36"),
        ):
            with self.subTest(price=price):
                self.assertEqual(OrderAutomation.order_price(price, "BUY"), buy)
                self.assertEqual(OrderAutomation.order_price(price, "sell"), sell)
        self.assertEqual(OrderAutomation.order_price("0.0001", "Buy"), "0.01")
        with self.assertRaisesRegex(ValueError, "rounds down to zero"):
            OrderAutomation.order_price("0.0099", "Sell")
        for price in ("0", "-1", "NaN", "Infinity", "invalid"):
            with self.subTest(price=price), self.assertRaises(ValueError):
                OrderAutomation.order_price(price, "Buy")
