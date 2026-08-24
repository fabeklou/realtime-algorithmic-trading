# Créer un nouveau topic de façon explicite
docker exec -it kafka-kraft kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic market-prices \
  --partitions 3 \
  --replication-factor 1

# Vérifier la création tu topic
docker exec -it kafka-kraft kafka-topics --describe --bootstrap-server localhost:9092 --topic market-prices
