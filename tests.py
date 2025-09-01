from pdct import process_df
import pandas as pd
import os


def default_test(test_name, input_file, test_solution):
    try:
        df_input = process_df.csv_to_dataframe(input_file)
        df_output = process_df.process_csv(df_input)

        temp_file = "tests/deleteme.csv"
        df_output.to_csv(
            temp_file,
            encoding='utf-8',
            index=False,
            header=True
        )

        df_answer = pd.read_csv(temp_file)
        os.remove(temp_file)
        df_solution = pd.read_csv(test_solution)

        pd.testing.assert_frame_equal(df_answer, df_solution)
        print(f"✅ SUCCESS: {test_name}")
    except Exception:
        print(f"❌ FAIL: {test_name}")


def messy_01():
    test_name = "messy_01"
    input_file = "tests/messy_01_input.csv"
    test_solution = "tests/messy_01_output.csv"

    default_test(test_name, input_file, test_solution)


def messy_02():
    test_name = "messy_02"
    input_file = "tests/messy_02_input.csv"
    test_solution = "tests/messy_02_output.csv"

    default_test(test_name, input_file, test_solution)


def messy_tests():
    messy_01()
    messy_02()


def main():
    messy_tests()

    print("All tests completed successfully!")


if __name__ == "__main__":
    main()
