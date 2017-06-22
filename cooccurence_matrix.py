import argparse
import csv
import pandas as pd
import numpy as np

class CooccurenceMatrix():

    def read_matrix(csv_file):
        df = pd.read_csv(csv_file)
        return df

    def gather_product_ids(df):
        sdf = df.sort_values("future_order_id")
        pids = sdf.loc[:,"product_id":"product_id"]
        pids_list = pids.values.flatten()
        pids_list.sort()
        unique_pids_set = set(pids_list)
        return unique_pids_set

    def generate_empty_dataframe(unique_pids):
        new_df = pd.DataFrame(0, index=unique_pids, columns=unique_pids)
        return new_df

    def populate_matrix(df, new_df):
        groups = df.groupby("future_order_id")
        for name, group in groups:
            product_list = group['product_id'].values
            anchor_item = group['product_id'].values[0]

            for item in product_list:
                current_freq = np.nan_to_num(new_df.loc[anchor_item, item])
                new_freq = current_freq + 1
                new_df.set_value(anchor_item, item, new_freq)
        return new_df

    def import_products_map(products_filename):
        this_dict = {}
        with open(products_filename) as csv_file:
            reader = csv.reader(csv_file, delimiter=',', quotechar='"')
            for row in reader:
                this_dict[row[0]] = row[1]
        return this_dict

    def view_results(sorted_results, product_map):
        print("RANK \t PRODUCT_ID \t PRODUCT_FREQ \t PRODUCT_NAME")
        count = 0
        for index, row in sorted_results.iterrows():
            this_product_id = row._name
            count = count + 1
            if this_product_id not in product_map:
               this_product_name = product_map.get(str(this_product_id))
               this_product_freq = row._data._values[0]
               if int(this_product_freq) > 0:
                  print("%(count)s \t %(this_product_id)s \t %(this_product_freq)s \t:: %(this_product_name)s" % locals())
            else:
               print("%(this_product_id)s n/a" % locals())


if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Co-occurrence Matrix')
   parser.add_argument('input_file', help='Valid CSV delimited file')
   parser.add_argument('product_id', help='Valid product id to construct co-occurence against')
   args = parser.parse_args()

   matrix = CooccurenceMatrix
   this_dataframe = matrix.read_matrix(args.input_file)
   unique_product_ids = matrix.gather_product_ids(this_dataframe)
   df_results = matrix.generate_empty_dataframe(unique_product_ids)

   df_results = matrix.populate_matrix(this_dataframe, df_results)
   product_map = matrix.import_products_map("test_data/products.csv")

   j = int(args.product_id)
   if j > 0 and j in df_results.index:
      result = df_results.loc[:, j:j]
      sorted_results = result.sort_values(j, ascending=False)
      matrix.view_results(sorted_results, product_map)
   else:
       print("Product id must be an integer and be present in matrix")
