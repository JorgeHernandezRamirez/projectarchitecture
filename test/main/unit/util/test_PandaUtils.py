import unittest

import pandas as pd
from numpy import nan

from projectarchitecture.util.PandaUtils import PandaUtils


class PandaUtilTest(unittest.TestCase):

    def test_should_fill_nan_with_None(self):
        dataframe_to_validate = PandaUtils.fill_nana_with_None(pd.DataFrame([["1", "Joge"],["2", nan]]))
        self.assertTrue(pd.DataFrame([["1", "Joge"],["2", None]]).equals(dataframe_to_validate))

    def test_should_fill_nan_with_None_NAT(self):
        dataframe_to_validate = PandaUtils.fill_nana_with_None(pd.DataFrame([["1", "Joge"],["2", nan], ["3", pd.NaT]]))
        self.assertTrue(pd.DataFrame([["1", "Joge"],["2", None],["3", None]]).equals(dataframe_to_validate))

    def test_should_validate_minus_operation(self):
        df1 = pd.DataFrame([["1", "Jorge"], ["2", "Jose"]], columns=["id", "name"])
        df2 = pd.DataFrame([["1", "Jorge"]], columns=["id", "name"])
        self.assertTrue(PandaUtils.minus(df1, df2, "id").equals(pd.DataFrame([["2", "Jose"]], columns=["id", "name"], index=[1])))

    def test_should_return_empty_df_if_are_the_same(self):
        df1 = pd.DataFrame([["1", "Jorge"], ["2", "Jose"]], columns=["id", "name"])
        self.assertTrue(PandaUtils.minus(df1, df1, "id").empty)

    def test_should_validate_intersertion_operation(self):
        df1 = pd.DataFrame([["1", "Jorge"], ["2", "Jose"]], columns=["id", "name"])
        df2 = pd.DataFrame([["1", "Jorge"]], columns=["id", "name"])
        self.assertTrue(PandaUtils.intersection(df1, df2, "id").equals(pd.DataFrame([["1", "Jorge"]], columns=["id", "name"], index=[0])))

    def test_should_count_row_size(self):
        self.assertEqual(2, PandaUtils.rows_size(pd.DataFrame([["1", "Jorge"], ["2", "Jose"]])))
        self.assertEqual(1, PandaUtils.rows_size(pd.DataFrame([["1", "Jorge"]])))
        self.assertEqual(0, PandaUtils.rows_size(pd.DataFrame([])))

    def test_return_empty_df_if_parameter_is_df_emtpy_fill_nana_with_None(self):
        self.assertTrue(PandaUtils.fill_nana_with_None(pd.DataFrame()).empty)

    def test_fill_nana_with_empty(self):
        self.assertTrue(pd.DataFrame(["", 1]).equals(
            PandaUtils.fill_nana_with_empty(pd.DataFrame([pd.np.nan, 1]))))

    def test_should_return_json_from_df(self):
        json = PandaUtils.to_json(pd.DataFrame([[1, 2]], columns=["A", "B"]))
        self.assertEqual(json, [{"A": 1, "B": 2}])



if __name__ == "__main__":
    unittest.main()