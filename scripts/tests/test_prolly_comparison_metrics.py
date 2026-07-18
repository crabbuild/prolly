import unittest

from scripts.prolly_process_metrics import parse_peak_rss


class ParsePeakRSSTest(unittest.TestCase):
    def test_parses_macos_bytes(self):
        text = "  98765432  maximum resident set size\n"
        self.assertEqual(parse_peak_rss(text), 98_765_432)

    def test_parses_gnu_kibibytes(self):
        text = "\tMaximum resident set size (kbytes): 12345\n"
        self.assertEqual(parse_peak_rss(text), 12_345 * 1024)

    def test_rejects_absent_metric(self):
        with self.assertRaisesRegex(ValueError, "peak RSS metric not found"):
            parse_peak_rss("user 0.10\nsystem 0.02\n")

    def test_rejects_malformed_metric(self):
        with self.assertRaisesRegex(ValueError, "malformed peak RSS metric"):
            parse_peak_rss("not-a-number maximum resident set size\n")

    def test_rejects_conflicting_metrics(self):
        text = (
            "100 maximum resident set size\n"
            "Maximum resident set size (kbytes): 2\n"
        )
        with self.assertRaisesRegex(ValueError, "conflicting peak RSS metrics"):
            parse_peak_rss(text)


if __name__ == "__main__":
    unittest.main()
