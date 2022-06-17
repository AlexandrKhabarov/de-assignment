from pyspark.sql import SparkSession

from de_assignment.arguments import parse_arguments
from de_assignment.extract import extract
from de_assignment.load import load  # noqa: WPS347
from de_assignment.transform import transform


def main(spark: SparkSession) -> None:
    arguments = parse_arguments()
    df = extract(spark, arguments.input_path)
    df = transform(df)
    load(df, arguments.output_path)
