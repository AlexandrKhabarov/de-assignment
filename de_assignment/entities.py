from dataclasses import dataclass


@dataclass
class Arguments:
    input_path: str
    output_path: str


@dataclass
class Signals:
    entity_id = 'entity_id'
    item_id = 'item_id'
    source = 'source'
    month_id = 'month_id'
    signal_count = 'signal_count'


@dataclass
class SignalsAgg:
    entity_id = 'entity_id'
    total_signals = 'total_signals'
    oldest_item_id = 'oldest_item_id'
    newest_item_id = 'newest_item_id'
