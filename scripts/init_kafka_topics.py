import os
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
from shared.kafka_topics import REQUIRED_TOPIC_LIST, TopicSpec

logger = logging.getLogger(__name__)


def build_admin_client(bootstrap_servers: str) -> AdminClient:
    """Instancie un client d'administration Kafka.

    Args:
        bootstrap_servers: Adresse(s) du broker, ex. "localhost:9092".

    Returns:
        Un AdminClient prêt à l'emploi.
    """
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def get_existing_topic_names(admin_client: AdminClient) -> set[str]:
    """Récupère les noms des topics déjà présents sur le cluster.

    Args:
        admin_client: Client connecté au broker.

    Returns:
        Ensemble des noms de topics existants.
    """
    topic_metadata = admin_client.list_topics(timeout=10)
    return set(topic_metadata.topics.keys())


def create_missing_topics(admin_client: AdminClient, specs: list[TopicSpec]) -> None:
    """Crée les topics du cluster qui n'existent pas encore.

    Args:
        admin_client: Client connecté au broker.
        specs: Liste des topics attendus, avec leur configuration.
    """
    existing = get_existing_topic_names(admin_client)

    new_topics = [
        NewTopic(
            topic=spec.name,
            num_partitions=spec.num_partitions,
            replication_factor=spec.replication_factor,
        )
        for spec in specs  # <- remplace par ta liste filtrée
        if spec.name not in existing
    ]

    if len(new_topics) == 0:
        logger.info("❇️ Rien à faire, pas de nouveaux topics à créer.")
        return

    # Cet appel est ASYNCHRONE : il retourne {nom: future}, ne crée rien
    # de confirmé à cette ligne précise.
    futures = admin_client.create_topics(new_topics)

    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info(f"✅ Topic créé : {topic_name}")
        except KafkaException as ex:
            logger.error(f"⛔ Erreur : {ex}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    kafka_external_port = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin_client = build_admin_client(bootstrap_servers=kafka_external_port)
    create_missing_topics(admin_client, REQUIRED_TOPIC_LIST)
