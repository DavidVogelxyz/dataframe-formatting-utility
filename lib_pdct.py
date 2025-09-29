import numpy as np
import pandas as pd
import re
import os
import sys


class process_df:
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
                    if isinstance(cell, str) \
                        or isinstance(cell, float) \
                            or isinstance(cell, int):
                        cell_str = str(cell).strip().lower()
                        if "date" == cell_str:
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
                    if 0 <= adj_row < df.shape[0] \
                            and 0 <= adj_col < df.shape[1]:
                        possible_date = df.iloc[adj_row, adj_col]
                        if isinstance(possible_date, str) \
                            and re.match(
                                r'\d{1,2}/\d{1,2}/\d{4}',
                                possible_date
                                ):
                            date_value = (
                                pd.to_datetime(possible_date)
                                .strftime('%Y-%m-%d')
                            )

                            # Set the word "date" to "NaN"
                            df.at[date_row, date_col] = np.nan
                            # Set the date itself to "NaN"
                            df.at[adj_row, adj_col] = np.nan

                            break

            return date_value
        except Exception as err:
            print("ERROR: failed to find the 'date' --", err)
            sys.exit("Exiting the program now.")

    def remove_superfluous(df):
        try:
            loop_superfluous = True

            while loop_superfluous is True:
                count = 0

                # Loop over rows to check number of non-NaN values
                for i, row in df.iterrows():
                    non_nan_mask = row.notna()
                    if non_nan_mask.sum() == 1:
                        # Get the column where the only non-NaN value is
                        col_idx = non_nan_mask.idxmax()
                        # Safe assignment without chained access
                        df.iat[i, col_idx] = np.nan
                        count += 1

                # Loop over columns to check number of non-NaN values
                for j in df.columns:
                    non_nan_mask = df[j].notna()
                    if non_nan_mask.sum() == 1:
                        # Get the row where the only non-NaN value is
                        row_idx = non_nan_mask.idxmax()
                        # Safe assignment using iat
                        df.iat[row_idx, j] = np.nan
                        count += 1

                # Stop the loop, only if no changes were made this round
                if count == 0:
                    loop_superfluous = False

            return df
        except Exception as err:
            print("ERROR: failed to clean the 'date' --", err)
            sys.exit("Exiting the program now.")

    def remove_duplicate_columns(df):
        # Create an empty set
        duplicate_columns = set()

        # Iterate through all the columns of dataframe
        for i in range(df.shape[1]):

            # Take column at index[i].
            left = df.iloc[:, i]

            # Iterate through all the remaining columns
            for j in range(i + 1, df.shape[1]):

                # Take column at index[j].
                right = df.iloc[:, j]

                # Check if two columns at i & j
                if left.equals(right):
                    duplicate_columns.add(df.columns.values[j])

        for col in duplicate_columns:
            df = df.drop(columns=[col])

        return df

    # consider looping through this until failure
    # potentially remove more than 1 row
    def check_bad_headers(df):
        arr_good_values = df.count(axis=1)
        mean_arr_good_values = np.mean(arr_good_values)

        if arr_good_values[0] < mean_arr_good_values * 0.69:
            df = df.iloc[1:]
            df.reset_index(drop=True, inplace=True)

        return df

    def remove_columns_with_no_headers(df):
        # Iterate through all the columns of dataframe
        for i in range(df.shape[1]):
            col = df.columns[i]

            if col is np.nan:
                df = df.drop(columns=[col])

        return df

    def date_cleanup(df, date_row, date_col):
        try:
            # Remove superfluous data
            df = process_df.remove_superfluous(df)

            # Drop empty rows
            df = df.dropna(axis=0, how='all')

            # Drop empty columns
            df = df.dropna(axis=1, how='all')

            # Reset the index before resetting column headers
            df.reset_index(drop=True, inplace=True)

            df = process_df.check_bad_headers(df)

            # Reset the column headers
            col_val = 0
            df.columns = df.iloc[col_val]
            df = df.drop(0)

            # Remove duplicate columns, without referencing headers
            df = process_df.remove_duplicate_columns(df)

            # At this point, remove any column without a header
            df = process_df.remove_columns_with_no_headers(df)

            return df
        except Exception as err:
            print("ERROR: failed to clean the data --", err)
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
        date_row, date_col = process_df.find_keyword_date(df)

        # Get the actual date from adjacent cells
        date_value = process_df.find_date_value(df, date_row, date_col)

        # Step 3: Clean up the dataframe of the date
        df = process_df.date_cleanup(df, date_row, date_col)

        # Step 4: split col merge
        if not df.columns.is_unique:
            df = process_df.split_col_merge(df)

        # Step 5: Add the date as a new column
        df = process_df.date_addback(df, date_value)

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
