# Create a function to get the files hash and file name from a directory
import hashlib
import os

import pandas as pd


def get_files_hash(data_directory: str) -> pd.DataFrame:
    """
    Calculate the hash value for each file in the specified data directory.

    Args:
        data_directory (str): The directory containing the files.

    Returns:
        pandas.DataFrame: A dataframe containing the file names and their corresponding hash values.
    """  # noqa: E501
    # Create an empty dataframe
    df_files_hash = pd.DataFrame(columns=["FILE_NAME", "HASH"])
    print("Reading files...")
    # Iterate for each file in src/data/data_directory and ignore hidden files
    for file in os.listdir(f"src/data/{data_directory}"):
        # Ignore hidden files
        if file.startswith("."):
            continue
        # Read the file
        with open(f"src/data/{data_directory}/{file}", "rb") as f:
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
    # Return the dataframe
    return df_files_hash


# Create a function to load SIMPLE_PROOF_20240306_S3 data
def load_simple_proof_20240306_s3(data_directory: str) -> pd.DataFrame:
    """
    Load the SIMPLE_PROOF_20240306_S3 data from the specified directory.

    Args:
        data_directory (str): The directory containing the data.

    Returns:
        pandas.DataFrame: A dataframe containing the data.
    """  # noqa: E501
    # Load the data
    df = pd.read_csv(f"src/data/{data_directory}/SIMPLE_PROOF_20240306_S3.csv")
    return df


# Create a function to validate the data from SIMPLE_PROOF_20240306_S3 data and the hash of the files  # noqa: E501
def validate_simple_proof_20240306_s3(
    validation_directory: str, raw_directory: str
) -> bool:
    """
    Validate the SIMPLE_PROOF_20240306_S3 data and the hash of the files.

    Args:
        data_directory (str): The directory containing the data.

    Returns:
        bool: True if the data is valid, False otherwise.
    """  # noqa: E501
    # Get the dataframe with the files and its hash
    df_files_hash = get_files_hash(raw_directory)
    # Files in local raw directory
    print("\nFiles in local raw directory:")
    # Print the first 5 rows of the dataframe
    print(df_files_hash.head())
    # Print the total files
    print("Total Files:", df_files_hash.shape[0])

    # Load the SIMPLE_PROOF_20240306_S3 data
    df = load_simple_proof_20240306_s3(validation_directory)
    # Files in S3 bucket
    print("\nFiles in S3 bucket:")
    # Print the first 5 rows of the dataframe
    print(df.head())
    # Print the total files
    print("Total Files:", df_files_hash.shape[0])

    # Remove duplicates by HASH
    df_files_hash = df_files_hash.drop_duplicates(subset="HASH")
    # Files in local raw directory without duplicates
    print("\nFiles in local raw directory without duplicates:", df_files_hash.shape[0])

    # Remove duplicates by FILE_NAME
    df = df.drop_duplicates(subset="FILE_NAME")
    # Files in S3 bucket without duplicates
    print("\nFiles in S3 bucket without duplicates:", df.shape[0])

    # Check if FILE_NAME is in the data and the number of files is equal to the number of files in the hash  # noqa: E501
    if (
        df["FILE_NAME"].isin(df_files_hash["FILE_NAME"]).all()
        and df.shape[0] == df_files_hash.shape[0]
    ):  # noqa: E501
        # Check if the hash of the files is the same as the file name in df["FILE_NAME"] without extension  # noqa: E501
        if df_files_hash["HASH"].isin(df["FILE_NAME"].str.split(".").str[0]).all():
            # Return True if the data is valid
            return True

    # Return False if the data is not valid
    return False


if __name__ == "__main__":
    # Validate the data from SIMPLE_PROOF_20240306_S3 data and the hash of the files
    if validate_simple_proof_20240306_s3("2_validation", "0_raw"):
        print("\nThe data is valid")
    else:
        print("\nThe data is not valid")
