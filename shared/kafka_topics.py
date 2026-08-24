from dataclasses import dataclass

@dataclass(frozen=True)
class TopicSpec:
    name: str
    num_partitions: int
    replication_factor: int

MARKET_PRICES_TOPIC = TopicSpec(
    name="market-prices",
    num_partitions=3,
    replication_factor=1,
)

MARKET_FEATURES_TOPIC = TopicSpec(
    name="market-features",
    num_partitions=3,
    replication_factor=1,
)

REQUIRED_TOPIC_LIST = [MARKET_PRICES_TOPIC, MARKET_FEATURES_TOPIC]
