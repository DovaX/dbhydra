import pandas as pd


def operation_sequence1(testing_table, *args):
    testing_table.delete(args[0])
    testing_table.delete(args[1])
    testing_table.update(args[2], args[3])

input_df = pd.DataFrame(
    {"name": ["John", "Lisa", "Charlie", "Rachel"], "age": [5, 67, 22, 144]}
).astype("object")

compare_df = pd.DataFrame({"name": ["Charlie", "Rachel"], "age": [22, 100]}).astype(
    "object"
)

test_case1 = (input_df, compare_df, operation_sequence1)


#TODO: add more test cases