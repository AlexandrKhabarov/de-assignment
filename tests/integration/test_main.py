from pyspark.sql import types as st

from de_assignment.entities import Signals, SignalsAgg
from de_assignment.main import main


def test_main(
    mocker,
    tmp_path,
    fake_spark,
    create_dataframe,
    assert_dataframes,
):
    input_path, output_path = (
        str(tmp_path / 'test'),
        str(tmp_path / 'expected'),
    )

    create_dataframe({
        (Signals.entity_id, st.LongType()): [1],
        (Signals.item_id, st.IntegerType()): [1],
        (Signals.source, st.IntegerType()): [1],
        (Signals.month_id, st.IntegerType()): [1],
        (Signals.signal_count, st.IntegerType()): [1],
    }).write.parquet(input_path)

    mocker.patch(
        'sys.argv',
        [
            'test_main.py',
            input_path,
            output_path,
        ],
    )

    main(fake_spark)

    assert_dataframes(
        expected_df=create_dataframe({
            (SignalsAgg.entity_id, st.LongType()): [1],
            (SignalsAgg.total_signals, st.IntegerType()): [1],
            (SignalsAgg.oldest_item_id, st.IntegerType()): [1],
            (SignalsAgg.newest_item_id, st.IntegerType()): [1],
        }),
        actual_df=fake_spark.read.parquet(output_path),
    )
