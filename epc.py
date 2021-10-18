import argparse
import fnmatch
import os
import zipfile

import pyarrow.csv
import pyarrow.parquet


def open_files(epc_zipfile, pattern):
    with zipfile.ZipFile(epc_zipfile, "r") as zip_file:
        matching_files = [
            member
            for member in zip_file.infolist()
            if fnmatch.fnmatch(member.filename, pattern)
        ]

        for matching_file in matching_files:
            yield zip_file.open(matching_file)


def csv_to_parquet(csv_file, parquet_file, column_types=None):
    table = pyarrow.csv.read_csv(
        csv_file,
        convert_options=pyarrow.csv.ConvertOptions(
            column_types=column_types or {}, auto_dict_encode=True
        ),
    )
    pyarrow.parquet.write_table(table, parquet_file)
    return None


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("epc_zipfile")
    parser.add_argument("output_path")
    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_args()

    certificate_files = open_files(
        args.epc_zipfile,
        "*/certificates.csv",
    )

    # https://epc.opendatacommunities.org/docs/guidance#glossary_domestic
    schema = {
        "LMK_KEY": pyarrow.string(),  # *not* integers
    }

    os.makedirs(args.output_path, exist_ok=True)

    for part_number, certificate_file in enumerate(certificate_files):
        parquet_file_path = os.path.join(
            args.output_path, f"part-{part_number:03}.parquet"
        )
        csv_to_parquet(certificate_file, parquet_file_path, schema)
        print(f"Written {parquet_file_path}")
