import csv
import io
import zipfile

import pyarrow
import pyarrow.parquet
import pytest

from epc import csv_to_parquet, open_files, parse_args


@pytest.fixture
def mock_epc_zipfile():
    file = io.BytesIO()
    with zipfile.ZipFile(file, "w") as zip_file:
        zip_file.writestr("authority-1/certificates.csv", b"certificates-1")
        zip_file.writestr("authority-1/recommendations.csv", b"recommendations-1")
        zip_file.writestr("authority-2/certificates.csv", b"certificates-2")
        zip_file.writestr("authority-2/recommendations.csv", b"recommendations-2")
    yield file


def test_open_files(mock_epc_zipfile):
    certificates = open_files(mock_epc_zipfile, "*/certificates.csv")
    assert [certificate.read() for certificate in certificates] == [
        b"certificates-1",
        b"certificates-2",
    ]


def test_csv_to_parquet(tmp_path):
    records = [
        ("John Doe", 30),
        ("Jane Doe", 31),
    ]

    csv_file_path = tmp_path / "records.csv"
    with open(csv_file_path, "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["name", "age"])
        writer.writerows(records)

    parquet_file_path = tmp_path / "records.parquet"
    schema = {
        "name": pyarrow.string(),
        "age": pyarrow.int8(),
    }
    csv_to_parquet(csv_file_path, parquet_file_path, schema)

    table = pyarrow.parquet.read_table(parquet_file_path)
    assert table.column_names == ["name", "age"]
    assert table["name"].type == "string"
    assert table["age"].type == "int8"


def test_parse_args():
    args = parse_args(["archive.zip", "destination"])
    assert args.epc_zipfile == "archive.zip"
    assert args.output_path == "destination"
