import json

import pandas as pd
from pandas.io.common import _NA_VALUES


class PandaUtils:

    @staticmethod
    def prepare_dataframes_to_diff(df1, df2, primary_key):
        columns_to_consider = PandaUtils.__intersection(df1.columns.tolist(), df2.columns.tolist())
        df1_intersection = PandaUtils.intersection(df1, df2, primary_key)
        df2_intersection = PandaUtils.intersection(df2, df1, primary_key)
        df1_subcolumns = df1_intersection.drop_duplicates(subset=[primary_key]).reindex(columns=columns_to_consider).drop_duplicates()
        df2_subcolumns = df2_intersection.drop_duplicates(subset=[primary_key]).reindex(columns=columns_to_consider).drop_duplicates()
        return (df1_subcolumns.set_index(primary_key).sort_index(), df2_subcolumns.set_index(primary_key).sort_index())

    @staticmethod
    def __intersection(list1, list2):
        return set(list1).intersection(list2)

    @staticmethod
    def diff_pd(df1, df2):
        assert (df1.columns == df2.columns).all(), \
            "DataFrame column names are different"
        if df1.equals(df2):
            return pd.DataFrame(columns=["oldvalue", "newvalue"])
        else:
            diff_mask = (df1 != df2) & ~(df1.isnull() & df2.isnull())
            ne_stacked = diff_mask.stack()
            changed = ne_stacked[ne_stacked]
            changed.index.names = ['id', 'col']
            difference_locations = pd.np.where(diff_mask)
            changed_from = df1.values[difference_locations]
            changed_to = df2.values[difference_locations]
            return pd.DataFrame({'oldvalue': changed_from, 'newvalue': changed_to}, columns=["oldvalue", "newvalue"],
                                index=changed.index)

    @staticmethod
    def intersection(df1, df2, field):
        if isinstance(field, list):
            return PandaUtils.intersection_field_list(df1, df2, field)
        return df1[df1[field].isin(df2[field])]

    @staticmethod
    def minus(df1, df2, field):
        return df1[~df1[field].isin(df2[field])]

    @staticmethod
    def fill_nana_with_None(df1):
        return df1.where(pd.notnull(df1), None)

    @staticmethod
    def fill_nana_with_empty(df1):
        return df1.where(pd.notnull(df1), "")

    @staticmethod
    def rows_size(df1):
        return df1.shape[0]

    @staticmethod
    def prueb2():
        pass

    @staticmethod
    def prepare_dataframe_to_insert(dataframe, columns_to_consider):
        dataframe_to_return = dataframe.reindex(columns=columns_to_consider).drop_duplicates()
        return PandaUtils.fill_nana_with_None(dataframe_to_return)

    @staticmethod
    def replace(dataframe, value_to_find, value_to_replace):
        return dataframe.replace(value_to_find, dataframe.replace([value_to_find], [value_to_replace]))

    @staticmethod
    def get_empty_df(columns = []):
        return pd.DataFrame(columns=columns)

    @staticmethod
    def get_na_values():
        return _NA_VALUES

    @staticmethod
    def to_json(df):
        return json.loads(df.to_json(orient='records'))