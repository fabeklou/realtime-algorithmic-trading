import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProducerConfig:
    """Configuration du producer de prix de marché.

    Attributes:
        kafka_bootstrap_servers: Adresse du broker Kafka.
        topic_name: Nom du topic cible.
        emit_interval_seconds: Délai entre deux publications successives.
    """
    kafka_bootstrap_servers: str
    topic_name: str
    emit_interval_seconds: float


def load_config() -> ProducerConfig:
    """Charge la configuration du producer depuis les variables d'environnement.

    Returns:
        Une instance de ProducerConfig prête à l'emploi.
    """
    return ProducerConfig(
        kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic_name=os.environ.get("MARKET_PRICES_TOPIC", "market-prices"),
        emit_interval_seconds=float(os.environ.get("EMIT_INTERVAL_SECONDS", "1.0")),
    )