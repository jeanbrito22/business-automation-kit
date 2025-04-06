from checker import validate_csv_against_schema, generate_corrected_csv

if __name__ == "__main__":
    validate_csv_against_schema("schema/sample_schema.json", "data/sample.csv")
    generate_corrected_csv("schema/sample_schema.json", "data/sample.csv", "data/sample_corrected.csv")