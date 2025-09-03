from pdct import process_df
import pandas as pd
import os


def default_test(test_name, test_desc, input_file, test_solution):
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
        print(f"✅ SUCCESS: {test_name} - {test_desc}")
    except Exception:
        print(f"❌ FAIL: {test_name} - {test_desc}")


def messy_01():
    test_name = "messy_01"
    test_desc = "Headers not row 0 (date above headers). Merge split columns."
    input_file = "tests/messy_01_input.csv"
    test_solution = "tests/messy_01_output.csv"

    default_test(test_name, test_desc, input_file, test_solution)


def messy_02():
    test_name = "messy_02"
    test_desc = "Headers row 0 (date on side). Merge split columns."
    input_file = "tests/messy_02_input.csv"
    test_solution = "tests/messy_02_output.csv"

    default_test(test_name, test_desc, input_file, test_solution)


def messy_03():
    test_name = "messy_03"
    test_desc = "Random words at top. Date embedded in data."
    input_file = "tests/messy_03_input.csv"
    test_solution = "tests/messy_03_output.csv"

    default_test(test_name, test_desc, input_file, test_solution)


def messy_04():
    test_name = "messy_04"
    test_desc = "Date embedded in data in new ways."
    input_file = "tests/messy_04_input.csv"
    test_solution = "tests/messy_04_output.csv"

    default_test(test_name, test_desc, input_file, test_solution)


def messy_05():
    test_name = "messy_05"
    test_desc = "Date embedded in data in more messy ways!"
    input_file = "tests/messy_05_input.csv"
    test_solution = "tests/messy_05_output.csv"

    default_test(test_name, test_desc, input_file, test_solution)


def messy_06():
    test_name = "messy_06"
    test_desc = "Date on top. Messiness everywhere."
    input_file = "tests/messy_06_input.csv"
    test_solution = "tests/messy_06_output.csv"

    default_test(test_name, test_desc, input_file, test_solution)


def messy_07():
    test_name = "messy_07"
    test_desc = "More mess and nonsense, but no curveballs."
    input_file = "tests/messy_07_input.csv"
    test_solution = "tests/messy_07_output.csv"

    default_test(test_name, test_desc, input_file, test_solution)


def messy_08():
    test_name = "messy_08"
    test_desc = "Drop a duplicate column. Also random data on the side."
    input_file = "tests/messy_08_input.csv"
    test_solution = "tests/messy_08_output.csv"

    default_test(test_name, test_desc, input_file, test_solution)


def messy_tests():
    messy_01()
    messy_02()
    messy_03()
    messy_04()
    messy_05()
    messy_06()
    messy_07()
    messy_08()


def main():
    messy_tests()

    print("All tests completed successfully!")


if __name__ == "__main__":
    main()
