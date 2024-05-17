import os
import re

import pandas as pd


def df_directory(directory):
    files = os.listdir(directory)
    # Remove hidden files
    files = [f for f in files if not f.startswith(".")]
    df = pd.DataFrame(files, columns=["FILE_NAME"])
    return df


dir_uno = "src/data/0_raw_uno"
dir_dos = "src/data/0_raw_dos"
dir_uploaded = "src/data/1_uploaded"

df_uno = df_directory(dir_uno)
df_dos = df_directory(dir_dos)
df_uploaded = df_directory(dir_uploaded)

# Print total data of df_uno and df_dos
print(f"Total {dir_uno}:", df_uno.shape[0])
print(f"Total {dir_dos}:", df_dos.shape[0])


def check_duplicates(df_uno, df_dos):
    duplicates = df_uno[df_uno["FILE_NAME"].isin(df_dos["FILE_NAME"])]
    return duplicates


files_duplicates = check_duplicates(df_uno, df_dos)

# Print len of files_duplicates
print("Total Duplicates: ", files_duplicates.shape[0])


def process_duplicates(df_uno, df_dos, files_duplicates):
    # If there aren't any duplicates, join df_uno and df_dos
    if len(files_duplicates) == 0:
        df = pd.concat([df_uno, df_dos])
        print(f"Total {dir_uno} + {dir_dos}: ", df.shape[0])
        # Sort df by FILE_NAME
        df = df.sort_values("FILE_NAME")
        # Save the report in the directory src/data/2_validation/TOTAL_FILES.csv
        df.to_csv("src/data/2_validation/TOTAL_FILES.csv", index=False)
        return df

    return None


# Get the total files without duplicates
df_total_files = process_duplicates(df_uno, df_dos, files_duplicates)

# Print len of df_total_files
print("Total Files without duplicates: ", df_total_files.shape[0])

# Load dataframe from file, Simple Proof
df_simple_proof = pd.read_csv("src/data/2_validation/SIMPLE_PROOF.csv")
print("Total Simple Proof Files with duplicates: ", df_simple_proof.shape[0])

# Remove duplicates from df_simple_proof
df_simple_proof = df_simple_proof.drop_duplicates()

# Print len of df_simple_proof
print("Total Simple Proof Files without duplicates: ", df_simple_proof.shape[0])

# Save the report in the directory src/data/2_validation/SIMPLE_PROOF_UNICOS.csv
df_simple_proof.to_csv("src/data/2_validation/SIMPLE_PROOF_UNICOS.csv", index=False)

# Missing files from df_total_files
df_missing_files_A = df_total_files[
    ~df_total_files["FILE_NAME"].isin(df_simple_proof["FILE_NAME"])
]

# Missing files from df_simple_proof
df_missing_files_B = df_simple_proof[
    ~df_simple_proof["FILE_NAME"].isin(df_total_files["FILE_NAME"])
]

# Total Missing Files
df_missing_files = pd.concat([df_missing_files_A, df_missing_files_B])
df_missing_files = df_missing_files.drop_duplicates()
# Sort df_missing_files by FILE_NAME
df_missing_files = df_missing_files.sort_values("FILE_NAME")

patron_uno = re.compile(r"^acta_\d+")
patron_dos = re.compile(r"^acta_dos_\d+")
df_missing_files["URL_TYPE"] = df_missing_files["FILE_NAME"].transform(
    lambda value: (
        "uno"
        if patron_uno.match(value)
        else ("dos" if patron_dos.match(value) else "uno:dos")
    )
)

df_missing_files["ACTA"] = df_missing_files["FILE_NAME"].transform(
    lambda value: ":".join(re.findall(r"\d+", value))
)

# Print len of df_missing_files
print("Total Missing Files: ", df_missing_files.shape[0])

# Save the report in the directory src/data/2_validation/MISSING_FILES.csv
df_missing_files.to_csv("src/data/2_validation/MISSING_FILES.csv", index=False)


# ---------------------------------------------------------------------------

print("--------------------------------------------------------------------")
print("--------------------------------------------------------------------")

# Total processed files from both URLs uno and dos without duplicates
df_proc_files = pd.read_csv("src/data/2_validation/TOTAL_FILES.csv")

# Concatenate df_proc_files with df_uploaded
df_proc_files = pd.concat([df_proc_files, df_uploaded])
df_proc_files.drop_duplicates(inplace=True)

# Print the shape of df_proc_files, total processed files
print("Total Processed Files: ", df_proc_files.shape[0])

# Check duplicates from df_proc_files
df_proc_duplicates = df_total_files[df_total_files.duplicated(subset=["FILE_NAME"])]

# Print the shape of duplicates, total duplicates
print("Total Duplicates Processed Files: ", df_proc_duplicates.shape[0])

# Create a dataframe from the file src/data/2_validation/ARCHIVOS_S3.csv
df_archivos_s3 = pd.read_csv("src/data/2_validation/ARCHIVOS_S3.csv")

# Print the shape of df_archivos_s3, total ARCHIVOS_S3
print("Total ARCHIVOS_S3:", df_archivos_s3.shape[0])

# Check duplicates from df_archivos_s3
df_archivos_s3_duplicates = df_archivos_s3[
    df_archivos_s3.duplicated(subset=["FILE_NAME"])
]

# Print the shape of duplicates, total duplicates
print("Total Duplicates ARCHIVOS_S3: ", df_archivos_s3_duplicates.shape[0])

# Missing files from df_proc_files
df_missing_files_A = df_proc_files[
    ~df_proc_files["FILE_NAME"].isin(df_archivos_s3["FILE_NAME"])
]

# Missing files from df_archivos_s3
df_missing_files_B = df_archivos_s3[
    ~df_archivos_s3["FILE_NAME"].isin(df_proc_files["FILE_NAME"])
]

# Total Missing Files
df_missing_files = pd.concat([df_missing_files_A, df_missing_files_B])
df_missing_files = df_missing_files.drop_duplicates()
# Sort df_missing_files by FILE_NAME
df_missing_files = df_missing_files.sort_values("FILE_NAME")

# Select FILE_NAME, FILE_NAME_SIMPLE_PROOF and BLOQUE where FILE_NAME_SIMPLE_PROOF is null and BLOQUE is null
# OR
df_missing_files = df_missing_files[
    df_missing_files["FILE_NAME_SIMPLE_PROOF"].isnull()
    | df_missing_files["BLOQUE"].isnull()
]
# df_missing_files = df_missing_files[
#     df_missing_files["FILE_NAME_SIMPLE_PROOF"].isnull()
#     | df_missing_files["BLOQUE"].isnull()
# ][["FILE_NAME", "FILE_NAME_SIMPLE_PROOF", "BLOQUE"]]

# AND
# df_missing_files = df_missing_files[
#     df_missing_files["FILE_NAME_SIMPLE_PROOF"].isnull()
#     & df_missing_files["BLOQUE"].isnull()
# ]

# Print the shape of df_missing_files, total missing files
print("Total Missing Files: ", df_missing_files.shape[0])

# Save the report in the directory src/data/2_validation/MISSING_FILES_S3.csv
df_missing_files.to_csv("src/data/2_validation/MISSING_FILES_S3.csv", index=False)

# For each file in df_missing_files, check if the file exists in the directory src/data/0_raw_uno, src/data/0_raw_dos or src/data/1_uploaded
# If the file exists, then create a new column in df_missing_files called "EXISTE_LOCAL" and set the value to "SI"
# If the file doesn't exist, then set the value to "NO"
df_missing_files["EXISTE_LOCAL"] = df_missing_files["FILE_NAME"].transform(
    lambda value: (
        "SI"
        if (
            os.path.exists(f"{dir_uno}/{value}")
            or os.path.exists(f"{dir_dos}/{value}")
            or os.path.exists(f"{dir_uploaded}/{value}")
        )
        else "NO"
    )
)

# Save the report in the directory src/data/2_validation/MISSING_FILES_S3.csv
df_missing_files.to_csv("src/data/2_validation/MISSING_FILES_S3.csv", index=False)


# Select FILE_NAME, FILE_NAME_SIMPLE_PROOF and BLOQUE where EXISTE_LOCAL is "SI"
df_missing_files = df_missing_files[df_missing_files["EXISTE_LOCAL"] == "SI"][
    ["FILE_NAME", "FILE_NAME_SIMPLE_PROOF", "BLOQUE"]
]

# Print the shape of df_missing_files, total missing files
print("Total Missing S3 Files: ", df_missing_files.shape[0])


# For each file in df_missing_files
# If the file exists in src/data/0_raw_uno, then copy the file to src/data/0_raw
# If the file exists in src/data/0_raw_dos, then copy the file to src/data/0_raw
# If the file exists in src/data/1_uploaded, then copy the file to src/data/0_raw
for index, row in df_missing_files.iterrows():
    file_name = row["FILE_NAME"]
    if os.path.exists(f"{dir_uno}/{file_name}"):
        os.system(f"cp {dir_uno}/{file_name} {dir_uno}/../0_raw/{file_name}")
    elif os.path.exists(f"{dir_dos}/{file_name}"):
        os.system(f"cp {dir_dos}/{file_name} {dir_dos}/../0_raw/{file_name}")
    elif os.path.exists(f"{dir_uploaded}/{file_name}"):
        os.system(f"cp {dir_uploaded}/{file_name} {dir_uploaded}/../0_raw/{file_name}")
