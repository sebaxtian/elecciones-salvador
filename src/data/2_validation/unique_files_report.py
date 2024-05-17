import hashlib
import os

import pandas as pd


def get_files_hash():
    # Create an empty dataframe
    df_files_hash = pd.DataFrame(columns=["FILE_NAME", "HASH"])
    print("Reading files...")
    # Iterate for each file in /src/data/total_files
    for file in os.listdir("src/data/total_files"):
        # Read the file
        with open(f"src/data/total_files/{file}", "rb") as f:
            # Create a hash from the file
            hash_file = hashlib.sha256(f.read()).hexdigest()
            # Add the file and the hash to the dataframe
            df_files_hash = pd.concat(
                [
                    df_files_hash,
                    pd.DataFrame([[file, hash_file]], columns=["FILE_NAME", "HASH"]),
                ],
                ignore_index=True,
            )
    print("Files readed")
    # Devolver el dataframe
    return df_files_hash


# Get the dataframe with the files and its hash
df_files_hash = get_files_hash()
# Print the first 5 rows of the dataframe
print(df_files_hash.head())
# Print the total files
print("Total Files:", df_files_hash.shape[0])

# Remove duplicates by HASH
df_files_hash = df_files_hash.drop_duplicates(subset="HASH")
# Print the total files without duplicates
print("Total Files without duplicates:", df_files_hash.shape[0])

# Guardar el dataframe en un archivo csv
df_files_hash.to_csv("src/data/2_validation/TOTAL_FILES_HASH.csv", index=False)
