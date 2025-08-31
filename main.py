from datetime import datetime as dt
import glob
import os
import pandas as pd
import sys


menu = {}
menu['1'] = "Re-export data."
menu['2'] = "Merge data."
menu['3'] = "Sort data."
menu['4'] = "Date to column."
menu['5'] = "Merge split columns."
menu['6'] = "the best option"
menu['7'] = "Exit"


def create_dir(dir):
    if not os.path.isdir(dir):
        os.makedirs(dir)


def csv_to_dataframe(file):
    try:
        df = pd.read_csv(file)
    except Exception as err:
        print("ERROR:", file, "failed to process into a dataframe --", err)

    return df


def get_date_from_csv(df, file):
    date = df.loc[0, "date"]
    date = pd.to_datetime(
        date,
        format="%m/%d/%Y"
    )

    print(file, "is dated", date)

    return date


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


def create_df_with_date_column(df, date):
    df.columns = df.iloc[1]
    df = df[2:].reset_index(drop=True)
    if not df.columns.is_unique:
        df = split_col_merge(df)
    df["date"] = date
    col = df.pop("date")
    df.insert(0, "date", col)

    return df


def process_date_from_df(df, path):
    date = get_date_from_csv(df, path)
    df = create_df_with_date_column(df, date)

    return df


def date_to_column(path, EXPORT_DIR):
    df = csv_to_dataframe(path)
    df = process_date_from_df(df, path)
    save_csv(df, EXPORT_DIR, path)


def split_cols_combine(path, EXPORT_DIR):
    df = csv_to_dataframe(path)
    df = split_col_merge(df)
    save_csv(df, EXPORT_DIR, path)


def save_csv(df, EXPORT_DIR, file):
    try:
        df.to_csv(
            os.path.join(EXPORT_DIR, os.path.basename(file)),
            encoding='utf-8',
            index=False,
            header=True
        )
    except Exception as err:
        print("ERROR:", file, "not saved --", err)


def menu_options(arg_count, path, filetype, selection):
    match selection:
        case "1":
            selection = "save"
            EXPORT_DIR = "clean"
            if not os.path.isdir(EXPORT_DIR):
                os.makedirs(EXPORT_DIR)

            if filetype == "dir":
                for file in glob.glob(os.path.join(path, "*.csv")):
                    process_csv(file, EXPORT_DIR, selection)
            if filetype == "file":
                process_csv(path, EXPORT_DIR, selection)
        case "2":
            selection = "merge"
            EXPORT_DIR = "clean"
            if not os.path.isdir(EXPORT_DIR):
                os.makedirs(EXPORT_DIR)

            try:
                df_temp
            except NameError:
                df_temp = []

            if filetype == "file" and arg_count == 1:
                sys.exit("ERROR: Only given 1 file. Nothing to merge.")

            if filetype == "dir":
                for file in glob.glob(os.path.join(path, "*.csv")):
                    prep_csv_for_merge(file, df_temp)
                merge_csv(path, EXPORT_DIR, df_temp, selection)

            if filetype == "file":
                prep_csv_for_merge(path, df_temp)
                merge_csv(path, EXPORT_DIR, df_temp, selection)
        case "3":
            selection = "sort"
            EXPORT_DIR = "sorted"
            if not os.path.isdir(EXPORT_DIR):
                os.makedirs(EXPORT_DIR)

            if filetype == "dir":
                for file in glob.glob(os.path.join(path, "*.csv")):
                    process_csv(file, EXPORT_DIR, selection)
            if filetype == "file":
                process_csv(path, EXPORT_DIR, selection)
        case "4":
            selection = "date"
            EXPORT_DIR = "clean"
            create_dir(EXPORT_DIR)

            if filetype == "dir":
                for file in glob.glob(os.path.join(path, "*.csv")):
                    date_to_column(path, EXPORT_DIR)
            if filetype == "file":
                date_to_column(path, EXPORT_DIR)
        case "5":
            selection = "splitcolumns"
            EXPORT_DIR = "clean"
            create_dir(EXPORT_DIR)

            if filetype == "dir":
                for file in glob.glob(os.path.join(path, "*.csv")):
                    split_cols_combine(path, EXPORT_DIR)
            if filetype == "file":
                split_cols_combine(path, EXPORT_DIR)
        case "6":
            selection = "best"
            EXPORT_DIR = "clean"
            create_dir(EXPORT_DIR)

            if filetype == "dir":
                sys.exit("Stop.")
            if filetype == "file":
                df = csv_to_dataframe(path)
                df = process_date_from_df(df, path)
                save_csv(df, EXPORT_DIR, path)
                print()
                print("The program saved the following dataframe:")
                #df.info()
                print(df)
                print()
        case "7":
            sys.exit("Exiting the program now.")
        case _:
            print("Unknown option selected. Skipping this path.")


def process_csv(file, EXPORT_DIR, selection):
    print("PROCESSING:", file)

    try:
        df = pd.read_csv(file)

        if selection == "sort":
            df = clean_data(df)
        if selection == "splitcolumns":
            df = split_columns_combine(df)

        df.to_csv(
            os.path.join(EXPORT_DIR, os.path.basename(file)),
            encoding='utf-8',
            index=False,
            header=True
        )
    except Exception as err:
        print("ERROR:", file, "not processed --", err)


def prep_csv_for_merge(path, df_temp):
    print("PREPPING FOR MERGE:", path)

    try:
        df = pd.read_csv(path)
        df_temp.append(df)
    except Exception as err:
        print("ERROR:", path, "not appended --", err)


def merge_csv(path, EXPORT_DIR, df_temp, selection, date):
    df_merged = pd.concat(df_temp, ignore_index=True)

    if selection == "merge":
        # FIX: what happens if `newname` includes `/`?
        newname = os.path.basename(path)
    if selection == "date":
        try:
            date
        except NameError:
            date = dt.now()

        date_string = date.strftime("%Y-%m-%d")
        newname = f'cleaned_date_template_{date_string}.csv'

    df_merged.to_csv(
        os.path.join(EXPORT_DIR, newname),
        encoding='utf-8',
        index=False,
        header=True
    )


def clean_data(df):
    df = df.sort_values('number')

    return df


def prep_csv_for_dating(file, df_temp):
    print("PREPPING FOR DATING:", file)

    df = pd.read_csv(file)

    date = df.loc[0, "date"]
    date = pd.to_datetime(
        date,
        format="%m/%d/%Y"
    )

    print("The date this template was filled is", date)

    df.columns = df.iloc[1]

    df = df[2:].reset_index(drop=True)

    df["date"] = date
    col = df.pop("date")
    df.insert(0, "date", col)

    df_temp.append(df)

    return date


def split_columns_combine(df):
    df_left = df[["number", "letter", "color"]]
    df_right = df[["number.1", "letter.1", "color.1"]]

    df_right.columns = df_left.columns

    df = pd.concat([df_left, df_right], ignore_index=True)

    return df


def menu_loop(arg_count, path, filetype):
    stoploop = False

    if stoploop is False:
        options = sorted(menu.keys())
        for entry in options:
            print(entry, "-", menu[entry])

        selection = input("Please select an option: ")

        print()

        if filetype == "dir":
            for file in glob.glob(os.path.join(path, "*.csv")):
                menu_options(arg_count, path, filetype, selection)

        if filetype == "file":
            menu_options(arg_count, path, filetype, selection)

        stoploop = True


def parse_args():
    # Exits if no argument is passed
    if len(sys.argv) < 2:
        sys.exit("ERROR: No file was passed. Please pass a file to continue.")

    arg_count = len(sys.argv) - 1

    for path in sys.argv[1:]:
        if not os.path.exists(path):
            print(path, "is not an existing path.")
            continue

        if os.path.isfile(path):
            filetype = "file"

            if not path.endswith(".csv"):
                print(path, "is not a CSV file, you silly goose!")
                continue
            else:
                print("What would you like to do with the following file?")

        if os.path.isdir(path):
            filetype = "dir"

            print("What would you like to do with the following directory?")

        print()
        print("   ", path)
        print()
        if filetype == "file":
            df = csv_to_dataframe(path)
            print("Dataframe:")
            #df.info()
            print(df)
            print()
        menu_loop(arg_count, path, filetype)
        print()


def main():
    parse_args()

    print("Processed all paths!")


if __name__ == "__main__":
    main()
