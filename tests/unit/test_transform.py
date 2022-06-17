import pytest
from pyspark.sql import types as st

from de_assignment.entities import Signals, SignalsAgg
from de_assignment.transform import transform


@pytest.mark.parametrize(
    (
        'test_data',
        'expected_data',
    ),
    [
        (
            {
                (Signals.entity_id, st.LongType()): [],
                (Signals.month_id, st.IntegerType()): [],
                (Signals.signal_count, st.IntegerType()): [],
                (Signals.item_id, st.IntegerType()): [],
            },
            {
                (SignalsAgg.entity_id, st.LongType()): [],
                (SignalsAgg.total_signals, st.IntegerType()): [],
                (SignalsAgg.oldest_item_id, st.IntegerType()): [],
                (SignalsAgg.newest_item_id, st.IntegerType()): [],
            },
        ),
        (
            {
                (Signals.entity_id, st.LongType()): [1],
                (Signals.month_id, st.IntegerType()): [1],
                (Signals.signal_count, st.IntegerType()): [1],
                (Signals.item_id, st.IntegerType()): [1],
            },
            {
                (SignalsAgg.entity_id, st.LongType()): [1],
                (SignalsAgg.total_signals, st.IntegerType()): [1],
                (SignalsAgg.oldest_item_id, st.IntegerType()): [1],
                (SignalsAgg.newest_item_id, st.IntegerType()): [1],
            },
        ),
        (
            {
                (Signals.entity_id, st.LongType()): [1, 1],
                (Signals.month_id, st.IntegerType()): [1, 2],
                (Signals.signal_count, st.IntegerType()): [1, 2],
                (Signals.item_id, st.IntegerType()): [1, 2],
            },
            {
                (SignalsAgg.entity_id, st.LongType()): [1],
                (SignalsAgg.total_signals, st.IntegerType()): [3],
                (SignalsAgg.oldest_item_id, st.IntegerType()): [1],
                (SignalsAgg.newest_item_id, st.IntegerType()): [2],
            },
        ),
        (
            {
                (Signals.entity_id, st.LongType()): [1, 2],
                (Signals.month_id, st.IntegerType()): [1, 1],
                (Signals.signal_count, st.IntegerType()): [1, 1],
                (Signals.item_id, st.IntegerType()): [1, 1],
            },
            {
                (SignalsAgg.entity_id, st.LongType()): [1, 2],
                (SignalsAgg.total_signals, st.IntegerType()): [1, 1],
                (SignalsAgg.oldest_item_id, st.IntegerType()): [1, 1],
                (SignalsAgg.newest_item_id, st.IntegerType()): [1, 1],
            },
        ),
        (
            {
                (Signals.entity_id, st.LongType()): [1, 1, 2],
                (Signals.month_id, st.IntegerType()): [1, 2, 1],
                (Signals.signal_count, st.IntegerType()): [1, 2, 1],
                (Signals.item_id, st.IntegerType()): [1, 2, 1],
            },
            {
                (SignalsAgg.entity_id, st.LongType()): [1, 2],
                (SignalsAgg.total_signals, st.IntegerType()): [3, 1],
                (SignalsAgg.oldest_item_id, st.IntegerType()): [1, 1],
                (SignalsAgg.newest_item_id, st.IntegerType()): [2, 1],
            },
        ),
    ],
)
def test_transform(
    test_data,
    expected_data,
    create_dataframe,
    assert_dataframes,
):
    actual_df = transform(create_dataframe(test_data))
    assert_dataframes(
        expected_df=create_dataframe(
            expected_data,
        ),
        actual_df=actual_df,
    )
