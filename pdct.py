from lib_pdct import process_df
import os
import sys


menu = {}
menu['1'] = "The ascended best option."
menu['2'] = "Exit"


def create_dir(dir):
    if not os.path.isdir(dir):
        os.makedirs(dir)


def best_option(df, path, EXPORT_DIR):
    df = process_df.process_csv(df)
    process_df.save_csv(df, EXPORT_DIR, path)


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

        df = process_df.csv_to_dataframe(path)
        print("Dataframe:")
        print(df)
        print()

        menu_loop(arg_count, path, filetype, df)


def main():
    parse_args()

    print("Processed all paths!")


if __name__ == "__main__":
    main()
