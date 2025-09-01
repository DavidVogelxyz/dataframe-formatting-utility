import numpy as np
import os
import pandas as pd
import re
import sys


menu = {}
menu['1'] = "The ascended best option."
menu['2'] = "Exit"


def create_dir(dir):
    if not os.path.isdir(dir):
        os.makedirs(dir)


def csv_to_dataframe(file):
    try:
        df = pd.read_csv(file, header=None)
    except Exception as err:
        print("ERROR:", file, "failed to process into a dataframe --", err)

    return df


def find_keyword_date(df):
    try:
        for i, row in df.iterrows():
            for j, cell in enumerate(row):
                #print(f"Checking: {i}, {j}, {cell}")

                if isinstance(cell, str) or isinstance(cell, float) or isinstance(cell, int):
                    cell_str = str(cell).strip().lower()
                    if 'date' in cell_str:
                        date_row, date_col = i, j
                        break

        return date_row, date_col
    except Exception as err:
        print("ERROR: failed to find the 'date' keyword --", err)
        sys.exit("Exiting the program now.")


def find_date_value(df, date_row, date_col):
    try:
        if date_row is not None and date_col is not None:
            for adj_row, adj_col in [
                (date_row + 1, date_col),
                (date_row - 1, date_col),
                (date_row, date_col + 1),
                (date_row, date_col - 1)
            ]:
                if 0 <= adj_row < df.shape[0] and 0 <= adj_col < df.shape[1]:
                    possible_date = df.iloc[adj_row, adj_col]
                    if isinstance(possible_date, str) and re.match(r'\d{1,2}/\d{1,2}/\d{4}', possible_date):
                        date_value = pd.to_datetime(possible_date).strftime('%Y-%m-%d')
                        df.at[adj_row, adj_col] = np.nan  # Set the date itself to "NaN"
                        break

        return date_value
    except Exception as err:
        print("ERROR: failed to find the 'date' --", err)
        sys.exit("Exiting the program now.")


def date_cleanup(df, date_row, date_col):
    try:
        df.at[date_row, date_col] = np.nan      # Set the word "date" to "NaN"
        df = df.dropna(axis=0, how='all')       # Drop rows
        df = df.dropna(axis=1, how='all')       # Drop columns
        df.reset_index(drop=True, inplace=True)
        col_val = 0
        df.columns = df.iloc[col_val]
        df = df.drop(0)

        return df
    except Exception as err:
        print("ERROR: failed to clean the 'date' --", err)
        sys.exit("Exiting the program now.")


def split_col_merge(df):
    sets_of_duplicates = df.columns.tolist().count(df.columns[0])
    group_size = len(df.columns) // sets_of_duplicates
    chunks = []

    # Split into chunks and concatenate
    for i in range(sets_of_duplicates):
        start = i * group_size
        end = start + group_size
        chunk = df.iloc[:, start:end]

        # Rename columns to match the first group
        chunk.columns = df.columns[:group_size]
        chunks.append(chunk)

    # Concatenate all chunks
    df = pd.concat(chunks, ignore_index=True)

    # Drop empty rows
    df = df.dropna(how='all')

    return df


def date_addback(df, date_value):
    try:
        if date_value:
            df.insert(0, 'date', date_value)

        return df
    except Exception as err:
        print("ERROR: failed to add back in the 'date' column --", err)
        sys.exit("Exiting the program now.")


def process_csv(df):
    # Get the coordinates of the "date" keyword
    date_row, date_col = find_keyword_date(df)

    # Get the actual date from adjacent cells
    date_value = find_date_value(df, date_row, date_col)

    # Step 3: Clean up the dataframe of the date
    df = date_cleanup(df, date_row, date_col)

    # Step 4: split col merge
    if not df.columns.is_unique:
        df = split_col_merge(df)

    # Step 5: Add the date as a new column
    df = date_addback(df, date_value)

    # Return the cleaned DataFrame
    return df


def save_csv(df, EXPORT_DIR, file):
    try:
        df.to_csv(
            os.path.join(EXPORT_DIR, os.path.basename(file)),
            encoding='utf-8',
            index=False,
            header=True
        )

        print()
        print("The program saved the following dataframe:")
        print(df)
        print()
    except Exception as err:
        print("ERROR:", file, "not saved --", err)


def best_option(df, path, EXPORT_DIR):
    df = process_csv(df)
    save_csv(df, EXPORT_DIR, path)


def menu_selection(arg_count, path, filetype, selection, df):
    match selection:
        case "1":
            selection = "best"
            EXPORT_DIR = "clean"
            create_dir(EXPORT_DIR)

            best_option(df, path, EXPORT_DIR)
        case "2":
            sys.exit("Exiting the program now.")
        case _:
            print("Unknown option selected. Skipping this path.")


def menu_loop(arg_count, path, filetype, df):
    stoploop = False

    if stoploop is False:
        options = sorted(menu.keys())
        for entry in options:
            print(entry, "-", menu[entry])

        selection = input("Please select an option: ")
        print()
        menu_selection(arg_count, path, filetype, selection, df)

        stoploop = True

    print()


def parse_args():
    # Exits if no argument is passed
    if len(sys.argv) < 2:
        sys.exit("ERROR: No file was passed. Please pass a file to continue.")

    arg_count = len(sys.argv) - 1

    for path in sys.argv[1:]:
        if not os.path.exists(path):
            print(path, "is not an existing path.")
            continue

        if os.path.isdir(path):
            print("Passing directories has been disallowed at this time.")
            continue

        if os.path.isfile(path):
            filetype = "file"

            if not path.endswith(".csv"):
                print(path, "is not a CSV file, you silly goose!")
                continue
            else:
                print("What would you like to do with the following file?")

        print()
        print("   ", path)
        print()

        df = csv_to_dataframe(path)
        print("Dataframe:")
        print(df)
        print()

        menu_loop(arg_count, path, filetype, df)


def main():
    parse_args()

    print("Processed all paths!")


if __name__ == "__main__":
    main()
