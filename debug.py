from lib_dfu import process_df
import os
import sys


def process_csv(df):
    # Get the coordinates of the "date" keyword
    date_row, date_col = process_df.find_keyword_date(df)

    # Get the actual date from adjacent cells
    date_value = process_df.find_date_value(df, date_row, date_col)
    print()
    print(f"The program has identified the following date value: {date_value}")

    # Step 3: Clean up the dataframe of the date
    print()
    print("This is the dataframe before cleanup:")
    print(df)
    df = process_df.date_cleanup(df, date_row, date_col)

    # Step 4: split col merge
    print()
    print(
        "This is the dataframe after cleanup, but before merging any columns:"
    )
    print(df)
    if not df.columns.is_unique:
        sets_of_duplicates = df.columns.tolist().count(df.columns[0])

        if sets_of_duplicates >= 1:
            df = process_df.split_col_merge(df, sets_of_duplicates)
        if sets_of_duplicates == 1:
            df = process_df.reshape_repeated_columns(df)
    print()
    print("This is the dataframe after attempting to merge columns:")
    print(df)

    # Step 5: Add the date as a new column
    df = process_df.date_addback(df, date_value)

    # Return the cleaned DataFrame
    return df


def parse_args():
    # Exits if no argument is passed
    if len(sys.argv) < 2:
        sys.exit("ERROR: No file was passed. Please pass a file to continue.")

    for path in sys.argv[1:]:
        if not os.path.exists(path):
            print(path, "is not an existing path.")
            continue

        if os.path.isdir(path):
            print("Passing directories has been disallowed at this time.")
            continue

        if os.path.isfile(path):
            if not path.endswith(".csv"):
                print(path, "is not a CSV file, you silly goose!")
                continue

        print(f"Troubleshooting \"{path}\".")
        print()
        df = process_df.csv_to_dataframe(path)
        print("The input dataframe looks like this:")
        print(df)
        df = process_csv(df)
        print()
        print("The output dataframe looks like this:")
        print(df)
        print()


def main():
    parse_args()

    print("Processed all paths!")


if __name__ == "__main__":
    main()
