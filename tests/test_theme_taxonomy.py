import os
import tempfile
import unittest

from src.themes.tag_new_tickers import _canonicalize_sheet_themes
from src.themes.theme_taxonomy import is_valid_tag, load_taxonomy, validate_path


class TaxonomyPathTests(unittest.TestCase):
    def test_the_taxonomy_loads_from_any_working_directory(self):
        # A relative config path read as {} from tests/, and lru_cache kept it.
        load_taxonomy.cache_clear()
        cwd = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                os.chdir(tmp)
                self.assertTrue(load_taxonomy())
                self.assertTrue(validate_path("AI / Data Center / Memory"))
        finally:
            os.chdir(cwd)
            load_taxonomy.cache_clear()


class StrictTagTests(unittest.TestCase):
    def test_a_bare_l1_is_a_tag_only_when_the_l1_has_no_children(self):
        self.assertTrue(validate_path("Space"))  # the lenient check accepts it
        self.assertFalse(is_valid_tag("Space"))
        self.assertTrue(is_valid_tag("Quantum Computing"))
        self.assertTrue(is_valid_tag("Space / Launch"))
        self.assertFalse(is_valid_tag("Space / No Such Leaf"))

    def test_bare_l1_sheet_labels_take_their_alias(self):
        kept, dropped = _canonicalize_sheet_themes(["Space", "Nuclear", "Cybersecurity"])
        self.assertEqual(dropped, [])
        for path in kept:
            self.assertTrue(is_valid_tag(path), path)
        self.assertNotIn("Space", kept)

    def test_a_label_that_is_already_a_tag_is_kept_as_is(self):
        kept, dropped = _canonicalize_sheet_themes(["Space / Launch", "Not A Theme"])
        self.assertEqual(kept, ["Space / Launch"])
        self.assertEqual(dropped, ["Not A Theme"])


if __name__ == "__main__":
    unittest.main()
