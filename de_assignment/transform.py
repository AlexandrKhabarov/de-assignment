from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as sf
from pyspark.sql import types as st

from de_assignment.entities import Signals, SignalsAgg


def transform(df: DataFrame) -> DataFrame:
    window = Window().partitionBy(Signals.entity_id)
    return (
        df.select(
            sf.col(Signals.entity_id),
            (
                sf.sum(Signals.signal_count)
                .over(window)
                .alias(SignalsAgg.total_signals)
                .cast(st.IntegerType())
            ),
            (
                sf.first(Signals.item_id)
                .over(
                    window.orderBy(
                        sf.col(Signals.month_id),
                        sf.col(Signals.item_id),
                    ),
                )
                .alias(SignalsAgg.oldest_item_id)
            ),
            (
                sf.first(Signals.item_id)
                .over(
                    window.orderBy(
                        sf.col(Signals.month_id).desc(),
                        sf.col(Signals.item_id),
                    ),
                )
                .alias(SignalsAgg.newest_item_id)
            ),
        )
        .distinct()
        .coalesce(1)
    )
