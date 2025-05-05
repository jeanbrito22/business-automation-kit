# utils/format_utils.py
def convert_spark_format_to_strptime(spark_format: str) -> str:
    return (spark_format
            .replace("yyyy", "%Y")
            .replace("MM", "%m")
            .replace("dd", "%d")
            .replace("HH", "%H")
            .replace("mm", "%M")
            .replace("ss", "%S"))
