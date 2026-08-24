import json
import logging
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer, KafkaException

from producers.market_price_producer.config import ProducerConfig, load_config

logger = logging.getLogger(__name__)

BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]


def fetch_real_price_event(symbol: str) -> dict | None:
    """Récupère le prix courant réel depuis l'API publique Binance.

    Args:
        symbol: Symbole au format Binance (ex. "BTCUSDT").

    Returns:
        Un dict représentant l'événement, sérialisable en JSON,
        ou None si l'appel API a échoué (erreur déjà loggée).
    """
    try:
        response = requests.get(
            BINANCE_TICKER_URL, params={"symbol": symbol}, timeout=5
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as ex:
        logger.error(f"⛔ Échec de récupération du prix pour {symbol} : {ex}")
        return None

    return {
        "symbol": symbol,
        "price": float(data["price"]),
        "event_timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }


def delivery_callback(err, msg) -> None:
    """Callback appelé par le producer après tentative de livraison.

    Args:
        err: None si succès, sinon un objet KafkaError.
        msg: Le message concerné (accessible même en cas d'erreur).
    """
    if err is not None:
        logger.error(f"⛔ Échec de livraison : {err}")
    else:
        logger.debug(
            f"✅ Livré sur {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}"
        )


def create_producer(config: ProducerConfig) -> Producer:
    """Instancie un producer Kafka.

    Args:
        config: Configuration chargée depuis l'environnement.

    Returns:
        Un Producer confluent-kafka prêt à l'emploi.
    """
    return Producer({
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
    })


def run(config: ProducerConfig) -> None:
    """Boucle principale : récupère et publie des prix réels en continu.

    Args:
        config: Configuration du producer.
    """
    producer = create_producer(config)
    logger.info(f"🚀 Producer démarré, publication sur '{config.topic_name}'")

    try:
        while True:
            for symbol in SYMBOLS:
                event = fetch_real_price_event(symbol)
                if event is None:
                    # Échec API déjà loggé dans fetch_real_price_event.
                    # On continue sur le symbole suivant plutôt que de
                    # bloquer tout le cycle pour une seule erreur réseau.
                    continue

                try:
                    producer.produce(
                        topic=config.topic_name,
                        key=symbol.encode("utf-8"),
                        value=json.dumps(event).encode("utf-8"),
                        callback=delivery_callback,
                    )
                except BufferError:
                    # La file interne du producer est pleine (rare en
                    # local à faible débit, mais bonne pratique de la
                    # gérer) : on force un flush partiel puis on continue.
                    logger.warning("⚠️ File producer pleine, flush forcé.")
                    producer.flush()

            # poll() traite les callbacks en attente (delivery reports)
            # sans bloquer indéfiniment.
            producer.poll(0)
            time.sleep(config.emit_interval_seconds)
    except KeyboardInterrupt:
        logger.info("🛑 Arrêt demandé, flush en cours...")
    except KafkaException as ex:
        logger.error(f"⛔ Erreur Kafka fatale : {ex}")
    finally:
        producer.flush()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run(load_config())