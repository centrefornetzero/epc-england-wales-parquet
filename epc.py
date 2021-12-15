import argparse
import fnmatch
import os
import zipfile

import pyarrow
import pyarrow.csv
import pyarrow.parquet

# https://epc.opendatacommunities.org/docs/guidance#glossary_domestic
# LMK_KEY is a string - first few rows look like integers
# Docs say `*_{CURRENT,POTENTIAL}` cols are integers but found floats
CERTIFICATE_SCHEMA = {
    "LMK_KEY": pyarrow.string(),
    "ADDRESS1": pyarrow.string(),
    "ADDRESS2": pyarrow.string(),
    "ADDRESS3": pyarrow.string(),
    "POSTCODE": pyarrow.string(),
    "BUILDING_REFERENCE_NUMBER": pyarrow.int64(),
    "CURRENT_ENERGY_RATING": pyarrow.string(),
    "POTENTIAL_ENERGY_RATING": pyarrow.string(),
    "CURRENT_ENERGY_EFFICIENCY": pyarrow.int64(),
    "POTENTIAL_ENERGY_EFFICIENCY": pyarrow.int64(),
    "PROPERTY_TYPE": pyarrow.string(),
    "BUILT_FORM": pyarrow.string(),
    "INSPECTION_DATE": pyarrow.date32(),
    "LOCAL_AUTHORITY": pyarrow.string(),
    "CONSTITUENCY": pyarrow.string(),
    "COUNTY": pyarrow.string(),
    "LODGEMENT_DATE": pyarrow.date32(),
    "TRANSACTION_TYPE": pyarrow.string(),
    "ENVIRONMENT_IMPACT_CURRENT": pyarrow.float64(),
    "ENVIRONMENT_IMPACT_POTENTIAL": pyarrow.float64(),
    "ENERGY_CONSUMPTION_CURRENT": pyarrow.float64(),
    "ENERGY_CONSUMPTION_POTENTIAL": pyarrow.float64(),
    "CO2_EMISSIONS_CURRENT": pyarrow.float64(),
    "CO2_EMISS_CURR_PER_FLOOR_AREA": pyarrow.float64(),
    "CO2_EMISSIONS_POTENTIAL": pyarrow.float64(),
    "LIGHTING_COST_CURRENT": pyarrow.float64(),
    "LIGHTING_COST_POTENTIAL": pyarrow.float64(),
    "HEATING_COST_CURRENT": pyarrow.float64(),
    "HEATING_COST_POTENTIAL": pyarrow.float64(),
    "HOT_WATER_COST_CURRENT": pyarrow.float64(),
    "HOT_WATER_COST_POTENTIAL": pyarrow.float64(),
    "TOTAL_FLOOR_AREA": pyarrow.float64(),
    "ENERGY_TARIFF": pyarrow.string(),
    "MAINS_GAS_FLAG": pyarrow.string(),
    "FLOOR_LEVEL": pyarrow.string(),
    "FLAT_TOP_STOREY": pyarrow.string(),
    "FLAT_STOREY_COUNT": pyarrow.int64(),
    "MAIN_HEATING_CONTROLS": pyarrow.string(),
    "MULTI_GLAZE_PROPORTION": pyarrow.float64(),  # int but values are floats, e.g 1.0
    "GLAZED_TYPE": pyarrow.string(),
    "GLAZED_AREA": pyarrow.string(),
    "EXTENSION_COUNT": pyarrow.float64(),  # int but values are floats, e.g 1.0
    "NUMBER_HABITABLE_ROOMS": pyarrow.float64(),  # int but values are floats, e.g 1.0
    "NUMBER_HEATED_ROOMS": pyarrow.float64(),
    "LOW_ENERGY_LIGHTING": pyarrow.int64(),
    "NUMBER_OPEN_FIREPLACES": pyarrow.int64(),
    "HOTWATER_DESCRIPTION": pyarrow.string(),
    "HOT_WATER_ENERGY_EFF": pyarrow.string(),
    "HOT_WATER_ENV_EFF": pyarrow.string(),
    "FLOOR_DESCRIPTION": pyarrow.string(),
    "FLOOR_ENERGY_EFF": pyarrow.string(),
    "FLOOR_ENV_EFF": pyarrow.string(),
    "WINDOWS_DESCRIPTION": pyarrow.string(),
    "WINDOWS_ENERGY_EFF": pyarrow.string(),
    "WINDOWS_ENV_EFF": pyarrow.string(),
    "WALLS_DESCRIPTION": pyarrow.string(),
    "WALLS_ENERGY_EFF": pyarrow.string(),
    "WALLS_ENV_EFF": pyarrow.string(),
    "SECONDHEAT_DESCRIPTION": pyarrow.string(),
    "SHEATING_ENERGY_EFF": pyarrow.string(),
    "SHEATING_ENV_EFF": pyarrow.string(),
    "ROOF_DESCRIPTION": pyarrow.string(),
    "ROOF_ENERGY_EFF": pyarrow.string(),
    "ROOF_ENV_EFF": pyarrow.string(),
    "MAINHEAT_DESCRIPTION": pyarrow.string(),
    "MAINHEAT_ENERGY_EFF": pyarrow.string(),
    "MAINHEAT_ENV_EFF": pyarrow.string(),
    "MAINHEATCONT_DESCRIPTION": pyarrow.string(),
    "MAINHEATC_ENERGY_EFF": pyarrow.string(),
    "MAINHEATC_ENV_EFF": pyarrow.string(),
    "LIGHTING_DESCRIPTION": pyarrow.string(),
    "LIGHTING_ENERGY_EFF": pyarrow.string(),
    "LIGHTING_ENV_EFF": pyarrow.string(),
    "MAIN_FUEL": pyarrow.string(),
    "WIND_TURBINE_COUNT": pyarrow.float64(),
    "HEAT_LOSS_CORRIDOR": pyarrow.string(),
    "UNHEATED_CORRIDOR_LENGTH": pyarrow.float64(),
    "FLOOR_HEIGHT": pyarrow.float64(),
    "PHOTO_SUPPLY": pyarrow.float64(),
    "SOLAR_WATER_HEATING_FLAG": pyarrow.string(),
    "MECHANICAL_VENTILATION": pyarrow.string(),
    "ADDRESS": pyarrow.string(),
    "LOCAL_AUTHORITY_LABEL": pyarrow.string(),
    "CONSTITUENCY_LABEL": pyarrow.string(),
    "POSTTOWN": pyarrow.string(),
    "CONSTRUCTION_AGE_BAND": pyarrow.string(),
    "LODGEMENT_DATETIME": pyarrow.timestamp("s"),
    "TENURE": pyarrow.string(),
    "FIXED_LIGHTING_OUTLETS_COUNT": pyarrow.float64(),
    "LOW_ENERGY_FIXED_LIGHT_COUNT": pyarrow.float64(),
    "UPRN": pyarrow.int64(),
    "UPRN_SOURCE": pyarrow.string(),
}


RECOMMENDATIONS_SCHEMA = {
    "LMK_KEY": pyarrow.string(),
    "IMPROVEMENT_ITEM": pyarrow.int64(),
    "IMPROVEMENT_SUMMARY_TEXT": pyarrow.string(),
    "IMPROVEMENT_DESCR_TEXT": pyarrow.string(),
    "IMPROVEMENT_ID": pyarrow.int64(),
    "IMPROVEMENT_ID_TEXT": pyarrow.string(),
    "INDICATIVE_COST": pyarrow.string(),
}


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
        convert_options=pyarrow.csv.ConvertOptions(column_types=column_types or {}),
    )
    pyarrow.parquet.write_table(table, parquet_file)
    return None


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("epc_zipfile")
    parser.add_argument("output_path")
    return parser.parse_args(args)


def convert_files(epc_zipfile, file_pattern, schema, output_path):
    files = open_files(
        epc_zipfile,
        file_pattern,
    )
    os.makedirs(output_path, exist_ok=True)

    for part_number, file in enumerate(files):
        parquet_file_path = os.path.join(output_path, f"part-{part_number:03}.parquet")
        csv_to_parquet(file, parquet_file_path, schema)
        print(f"Written {parquet_file_path}")


if __name__ == "__main__":
    args = parse_args()

    convert_files(
        args.epc_zipfile,
        "*/certificates.csv",
        CERTIFICATE_SCHEMA,
        os.path.join(args.output_path, "certificates"),
    )

    convert_files(
        args.epc_zipfile,
        "*/recommendations.csv",
        RECOMMENDATIONS_SCHEMA,
        os.path.join(args.output_path, "recommendations"),
    )
