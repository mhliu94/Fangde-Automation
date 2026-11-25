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
            ("2.360000000000000000000000000001", "2.37", "2.36"),
            ("23625e-4", "2.37", "2.36"),
        ):
            with self.subTest(price=price):
                self.assertEqual(OrderAutomation.order_price(price, "BUY"), buy)
                self.assertEqual(OrderAutomation.order_price(price, "sell"), sell)
        for price in ("0", "-1", "NaN", "Infinity", "invalid"):
            with self.subTest(price=price), self.assertRaises(ValueError):
                OrderAutomation.order_price(price, "Buy")

    def test_sub_dollar_prices_use_the_four_decimal_grid(self):
        for requested, expected in (("0.0001", "0.0001"), ("0.0099", "0.0099"), ("0.999900", "0.9999")):
            for side in ("Buy", "Sell"):
                with self.subTest(price=requested, side=side):
                    self.assertEqual(OrderAutomation.order_price(requested, side), expected)
        with self.assertRaisesRegex(ValueError, r"multiple of \$0\.0001"):
            OrderAutomation.order_price("0.010001", "Sell")
