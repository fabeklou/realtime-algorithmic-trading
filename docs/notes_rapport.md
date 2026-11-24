
## Thème: Conception et développement d'une plateforme d'analyse de flux de données en temps réel et de modélisation prédictive pour le trading algorithmique.

### Description du Projet:
> Ce projet consiste à concevoir et développer une architecture d’ingestion, traitement et valorisation des flux massifs et continu de données financières.
---
**Pour ton rapport**

C'est exactement le genre de décision à documenter explicitement dans une section "choix techniques" : "Nous avons privilégié des données de marché réelles (API publique Binance) plutôt que des données simulées, afin de garantir que l'évaluation du modèle ML reflète un signal réel et soit scientifiquement défendable, au prix d'une dépendance à la disponibilité et aux limites de taux (rate limits) d'un service externe." — ça montre exactement la maturité de raisonnement attendue par un jury.
---
**Ce que ça te permet de conclure pour ton rapport**
Le producer est résilient aux pannes transitoires du broker : aucun crash, aucune perte de messages générés pendant la coupure (ils restent dans le buffer interne de librdkafka, avec une limite de taille configurable — bon point à creuser si le temps permet).
Aucune intervention manuelle nécessaire pour la reprise — ce qui est exactement le comportement voulu pour un système qu'on veut opérer sans surveillance constante (rappelle-toi la question qu'on s'était posée : "si ce système tombe à 3h du matin, comment sait-on ce qui se passe ?" — ici, la réponse honnête est : il se répare tout seul, sans même besoin d'alerte à 3h du matin, tant que la coupure reste temporaire).
Limite à noter honnêtement : si la coupure avait duré plus longtemps que la capacité du buffer interne (queue.buffering.max.messages, valeur par défaut assez généreuse), des messages auraient commencé à être rejetés avec une BufferError — c'est justement le try/except BufferError qu'on a mis dans le code qui gérerait ce cas. Bon point à mentionner comme "limite connue" dans le rapport plutôt que de prétendre une résilience infinie.
---
**Point à noter pour ton rapport** : ce genre de piège (process principal du conteneur qui nécessite une TTY interactive, incompatible avec un démarrage en arrière-plan) est un vrai sujet d'ops Docker — bon exemple à mentionner si le jury demande "quelles difficultés techniques avez-vous rencontrées".
---
Pourquoi c'est important de le noter pour ton rapport : c'est exactement le genre de réflexe "environnements de prod" qu'on avait évoqué en tout début de conversation — ne redémarrer que le strict nécessaire, pour éviter une coupure de service évitable sur des composants qui n'ont pas changé. Bon point à mentionner si le jury demande "comment gérez-vous les déploiements/mises à jour de configuration ?".
---
**Pourquoi c'est un bon point pour ton rapport**

C'est un exemple concret et réel de contrat de schéma mal aligné entre producer et consumer — exactement le genre de problème qu'on avait évoqué en toute première partie de notre discussion (gestion des schémas, qualité des données). Ici ça vient d'une différence subtile entre le format par défaut de datetime.isoformat() en Python et les attentes strictes de typage temporel de Flink SQL. Bon exemple à citer si le jury demande "quelles difficultés avez-vous rencontrées et comment les avez-vous résolues".
---
**Un point à noter pour ton rapport**

C'est un excellent exemple concret d'un problème classique en ingénierie de données : "ISO-8601" n'est pas un format unique et univoque — c'est une famille de formats avec des variantes (RFC 3339 en étant une sous-norme plus stricte). Deux systèmes qui prétendent tous les deux "supporter ISO-8601" peuvent quand même être incompatibles sur des détails comme celui-ci. C'est exactement le genre de piège d'intégration que mentionnent les praticiens seniors quand on parle de "contrats de schéma" entre producer et consumer.
---
**Point à retenir et à noter dans ton rapport** : scan.startup.mode = 'latest-offset' n'est fiable que pour un consumer group jamais utilisé auparavant. Pour forcer une vraie relecture "à partir de maintenant" lors d'un redémarrage de développement/debug, il aurait fallu soit changer le group.id à chaque tentative, soit utiliser kafka-consumer-groups --reset-offsets pour repositionner explicitement le groupe. Bon exemple concret à mentionner si le jury demande "comment gérez-vous la reprise après erreur d'un pipeline streaming".
---
**Pourquoi c'est un excellent point pour ton rapport**

C'est un vrai sujet de conception à mentionner : le choix du nombre de partitions et de la clé de partitionnement a un impact direct sur la correction fonctionnelle d'un pipeline event-time, pas seulement sur la performance. Avec seulement 3 clés distinctes sur 3 partitions, tu es dans une zone à risque de déséquilibre. Deux façons de le présenter dans ton rapport :

soit tu gardes 3 partitions + idle-timeout (solution robuste et réaliste — beaucoup de systèmes de production ont ce problème avec un faible nombre de clés) ;
soit tu notes en "limite connue / amélioration future" qu'un partitionnement avec plus de clés (plus de symboles) réduirait ce risque naturellement.
---
Reformulation précise : "Avec un watermark de 30 secondes, Flink attend plus longtemps avant de clôturer chaque fenêtre — donc plus de tolérance pour les événements en retard (meilleure exactitude si le réseau est instable), mais chaque résultat de fenêtre arrive avec jusqu'à 30 secondes de retard par rapport au temps réel, au lieu de 5. Compromis classique : exactitude vs fraîcheur du résultat." Pour du trading, où la fraîcheur du signal compte énormément, un watermark trop long serait probablement contre-productif — bon point à noter.
---
Cause principale : format de date incompatible avec le parseur JSON rapide de ClickHouse

Regarde l'erreur : expected '"' before: 'Z"' sur le champ window_start. Ton JSON contient "2026-08-24 22:54:00Z" (avec un Z final, provenant de TIMESTAMP_LTZ côté Flink). Le parseur JSON par défaut de ClickHouse pour DateTime64 attend un format strict "YYYY-MM-DD HH:MM:SS.sss", sans lettre de fuseau — il ne connaît pas nativement la notation Z. C'est encore une variation du même problème de format de date qu'on a déjà rencontré deux fois (Python → Flink), qui revient une troisième fois entre Flink → ClickHouse. Pattern à noter explicitement dans ton rapport : les incompatibilités de format temporel entre systèmes sont un point de friction récurrent et non anecdotique en ingénierie de données réelle.
---
1. Qui écrit avant l'ack ? Ce n'est pas le "controller", c'est le leader de la partition et ses réplicas synchronisés (les followers à jour). Le controller gère les métadonnées du cluster (qui est leader de quoi), pas l'écriture des messages.

2. "Aucun message n'est jamais perdu". C'est vrai seulement s'il existe des réplicas. Dans ton projet, tu as un seul broker et replication-factor: 1, donc acks=all n'ajoute aucune protection par rapport à acks=1. Le paramètre prend son sens à partir de 3 brokers, avec min.insync.replicas=2. C'est une limite à assumer dans ton rapport.
---
Tu pourras écrire dans le rapport : "persistance Kafka via volume nommé, alignée sur log.dirs".
---
**Une limite à connaître pour ton projet.** Ton event_timestamp est généré par ton producer au moment où il interroge Binance. Le timestamp Kafka est lui aussi posé par le producer (CreateTime, visible dans tes logs). Les deux sont donc très proches ici. L'event time apporterait plus avec un horodatage venant de la source, par exemple l'heure du trade fournie par Binance. C'est une amélioration à citer dans les perspectives du rapport.
---
**À retenir pour le rapport** : avec HOP, le volume de sortie est constant (3 lignes par 10 s) et indépendant du volume d'entrée. Le coût est que chaque événement est compté 6 fois, une fois par fenêtre qui le contient.
---
Ta phrase sur la réconciliation correspond au contournement qu'on avait évoqué : écrire chaque changement d'état comme une nouvelle ligne et retrouver la plus récente par order_id. Cela fonctionne, mais c'est du calcul à chaque lecture. D'où l'idée de garder un petit Postgres pour l'état des ordres, dans la roadmap de ton rapport.
---
**À faire ensuite, pour la reproductibilité** : quand les lignes arrivent, mets à jour clickhouse/init/01_create_tables.sql avec les mêmes réglages (-v2, kafka_skip_broken_messages, date_time_input_format). Sinon, un nouveau volume ClickHouse recréerait la table avec l'ancienne définition cassée. Ce point peut aller dans la section "difficultés rencontrées" du rapport.
---
**Une correction de ma part.** Au Jalon 2, je t'ai expliqué que les anciens offsets du consumer group faussaient le démarrage. C'était inexact : avec latest-offset, Flink ignore les offsets du groupe. Ne reprends pas cette explication dans ton rapport. Les échecs à répétition venaient des messages au mauvais format, produits par le producer avant ta correction.
---
**Réponse à "où bloquer ?"** À plusieurs endroits, c'est la défense en profondeur. Les règles simples vont le plus tôt possible (producer), pour ne pas polluer Kafka. Flink garde un WHERE comme filet de sécurité, car tu ne contrôleras pas toujours les producers. ClickHouse peut aussi porter une contrainte. Le nettoyage statistique se fait plus tard, dans la préparation des données du modèle, où l'on a tout l'historique. Pour le rapport : "validation en couches, avec dead-letter queue pour l'auditabilité".
---
L'API de bougies de Binance (/api/v3/klines) renvoie l'historique en OHLCV, et le site data.binance.vision propose des archives par jour ou par mois. Vérifie dans leur doc les intervalles disponibles (je crois que 1 s existe sur le spot) et les limites de requêtes. Trois points à respecter :

Calculer les features comme le pipeline live. Le modèle s'entraîne sur des moyennes et volatilités de fenêtres de 60 s, avec un pas de 10 s. Si les features historiques sont calculées autrement, le modèle verra en production des données qui n'ont pas la même distribution : c'est le training-serving skew. Des bougies de 1 s s'en approchent sans être identiques, car ton producer échantillonne le prix environ une fois par seconde, avec de la gigue. Documente cet écart dans le rapport.
Utiliser le live pour contrôler le modèle. Le pipeline actuel devient la source d'inférence et de monitoring : tu compares la distribution des features live à celle de l'entraînement. C'est ton argument de drift.
Découper dans le temps, sans mélange. Les fenêtres HOP se chevauchent : deux lignes voisines partagent 50 s sur 60. Un découpage aléatoire ferait fuiter l'information entre entraînement et test. Il faut un découpage chronologique, avec un trou entre les deux.
---
En pratique : à 5 minutes, le rendement moyen d'entraînement est presque nul, donc les deux premières baselines seront quasi identiques. Si le marché a monté pendant ton entraînement, la moyenne est biaisée et elle te pénalisera sur le test. Reporte les trois dans le rapport.
---
**Backtest net de frais** : on simule des transactions quand la prédiction dépasse un seuil, et on mesure le gain après frais. Les frais Binance sont d'environ 0,1 % par transaction au tarif de base (à vérifier). Si la volatilité à 5 minutes est du même ordre, les frais peuvent absorber tout le gain. C'est un point à traiter franchement dans le rapport.
---
**Conséquence pour le modèle** : en production, la décision est prise à environ t + 15 s, pas à t. Le prix de référence de la cible doit donc être celui que le trader peut réellement obtenir à cet instant. L'écart est petit face à 5 minutes, mais à noter dans le rapport.
---
**Piège sur avg_price** : c'est une moyenne sur 60 s, donc un prix lissé et décalé d'environ 30 s par rapport au prix réel. Une cible calculée sur deux moyennes est un rendement de prix lissés. Avec les bougies historiques, utilise le prix de clôture à t et à t + 5 min, et documente la différence avec le live.
---
La formule

avg_price(t) couvre [t-60 s, t] et avg_price(t-60 s) couvre [t-120 s, t-60 s]. Les deux fenêtres ne se chevauchent pas, donc il n'y a pas de fuite entre elles.
Comme le pas du HOP est de 10 s, la ligne à t - 60 s existe exactement quand il n'y a pas de trou. On peut donc la chercher par égalité de window_end, par symbole.
Cette feature est un rendement entre deux moyennes de prix consécutives, pas entre deux prix instantanés. C'est un détail à noter dans le rapport.
---
**Ce que je te recommande pour le rapport**

Garde la précision directionnelle comme métrique principale, si tu la justifies comme tu l'as fait, et ajoute deux compléments :

le RMSE comparé à celui de la baseline, pour la grandeur de l'erreur ;
un backtest net de frais, qui fait le lien avec le critère métier.
---
Le gain brut attendu par trade vaut (2p − 1) × mouvement, soit 0,04 × 0,05 % = 0,002 %. Après frais, on obtient environ −0,198 % par trade.

**La conclusion à retenir pour le rapport**

Même avec une précision de 100 %, le gain brut serait de 0,05 % contre 0,2 % de frais. Aucun niveau de précision ne compense des mouvements plus petits que les frais. C'est pourquoi la précision seule ne permet pas de conclure : il faut l'associer à l'amplitude des mouvements et aux coûts.

Deux nuances

Le mouvement de 0,05 % est une moyenne. Un modèle peut ne trader que lorsque le mouvement prédit dépasse largement les frais. Il fait alors peu de trades, et il faut mesurer s'ils sont assez justes. C'est le seuil du backtest dont on a parlé.
Les frais dépendent du compte (palier de volume, réduction avec le BNB, ordres maker). Vérifie les valeurs actuelles dans la doc Binance avant d'écrire un chiffre dans le rapport.

Ce point mérite une section franche dans ton rapport : "à cet horizon, les frais dominent le signal, et le modèle est évalué sur sa capacité à prédire, pas sur un profit réel".
---
Un réseau sur séquences (LSTM) est une extension possible. Mets-le dans les perspectives du rapport, sans le construire maintenant.
---
Un point de vigilance pour la suite

Ces bougies sont des données agrégées par seconde. Ton producer échantillonnait le prix environ une fois par seconde. Pour les features, calcule la moyenne et l'écart-type sur le prix de clôture (close) de chaque seconde. Ce n'est pas identique au live (un prix instantané avec de la gigue), et il faut le documenter dans le rapport comme source de décalage entre entraînement et production.
---
Deux écarts avec le live, à noter dans le rapport

trade_count est la somme des transactions Binance sur 60 s. Le live compte les prix échantillonnés (environ 60). Ce n'est donc pas la même mesure, et je ne l'utiliserais pas comme feature commune.
range_pct utilise high et low des bougies, alors que le live n'a que des échantillons. Pour l'avoir en live, il faudrait ajouter MIN(price) et MAX(price) au SQL Flink.
---
L'écart-type de la cible est de 0,107 % (BTC), 0,115 % (BNB) et 0,147 % (ETH). C'est la RMSE de ta baseline. Ces valeurs sont inférieures aux frais d'un aller-retour (0,2 %, ou 0,15 % avec le BNB), et un mouvement typique est à peu près la moitié de ces frais. Garde ce constat pour la section sur la rentabilité du rapport.
---
**Choix à noter pour le rapport**

vol_rel au lieu de price_volatility. L'écart-type brut est en dollars (BTC ≈ 78 000 $, BNB ≈ 700 $). Les valeurs ne seraient pas comparables entre symboles. ret_1m et range_pct sont déjà relatives.
trade_count n'est pas une feature. Il n'a pas d'équivalent dans le live. Ajouter range_pct au live demande MIN(price) et MAX(price) dans le SQL Flink, à faire avant une éventuelle inférence en temps réel.
Les trois symboles sont regroupés dans un seul modèle, ce qui est possible parce que les features sont relatives. Un modèle par symbole est une variante à tester.
La validation est un seul bloc. Le walk-forward avec TimeSeriesSplit(gap=...) donnerait plusieurs mesures. C'est une amélioration à ajouter si le temps le permet.
Le test se fait une seule fois, à la fin, avec le modèle et les réglages figés.
---
**Ce que tu peux écrire dans le rapport** : avec ces trois features, à l'horizon de 5 minutes et sur ce bloc de validation, aucun modèle ne montre de gain mesurable sur la baseline. C'est cohérent avec des rendements à court terme proches du bruit, et avec les frais (0,15 à 0,2 % pour un aller-retour) supérieurs au mouvement typique (≈ 0,13 %). Ne conclus pas que « le marché est imprévisible » : tu as testé un seul horizon, trois features et un seul bloc de validation.
---
**Essais multiples.** Un deuxième horizon est un essai de plus, fait après un échec. Il ne faut pas cacher le 5 min : le rapport présentera les deux horizons, ainsi que la décision de les tester. Le test final n'est lancé qu'une fois, sur des choix décidés avant de le voir.
---
Le bon point de comparaison est la corrélation de rang. À 5 min, la persistance fait 0,292, Ridge 0,289 (donc aucun gain), et la forêt et LightGBM 0,322. L'apport des modèles non linéaires est réel mais petit (≈ +0,03). Pour le rapport : « la volatilité récente prédit la volatilité future (rang ≈ 0,3), et les modèles non linéaires ajoutent un petit gain ».
---
Avant le bloc de test, fige ces choix

Écris-les dans docs/notes_rapport.md avant de lancer le test, pour que le rapport montre que rien n'a été ajusté après coup :

Les horizons (5 et 60 min), la liste des features et les réglages des modèles.
La mesure de succès. En volatilité : la corrélation de rang et le R² contre la persistance calibrée. En direction : le résultat nul.
Le décompte honnête des essais faits sur la validation (2 horizons × 2 problèmes × 3 modèles, plus la variante avec features de contexte).
---
Ce que montrent tes sorties (cellule 8, sans les features de contexte)
La forêt bat la persistance calibrée dans chacun des 3 symboles. Le gain en Spearman est de +0,03 pour BNB, BTC et ETH à 5 min, et de +0,02 à 60 min. Il n'est donc pas dû à un seul symbole.
Le R² de la persistance calibrée vaut 0,119 à 5 min et 0,046 à 60 min. La forêt fait 0,146 et 0,066, soit un gain réel mais modeste. Ridge à 5 min (0,103) est sous la persistance calibrée. Pour le rapport : « la volatilité récente explique l'essentiel du signal, les modèles non linéaires ajoutent un petit gain ».
---
Les trois variantes : si per_symbol ou pooled+symbol n'améliore pas nettement pooled, garde le modèle groupé, plus simple, et cite ta remarque comme limite testée. Si une variante gagne, c'est ta réflexion qui l'a trouvée, et tu le dis dans le rapport.
---
On garde donc le modèle groupé sans symbole, le plus simple. Pour le rapport : « l'hypothèse d'un modèle par symbole a été testée, sans gain mesurable ». Ton intuition était légitime, et la validation la tranche.

Ce qui reste valable avec prudence

Un seul bloc de validation (septembre) a servi à choisir les features et le modèle. Le test sert précisément à confirmer le résultat.
Le R² reste modeste (0,10 à 0,18). La volatilité future n'est que partiellement prévisible.
---
---
Choix figés avant le test. Modèle volatilité : forêt aléatoire groupée (réglages de build_models), 7 features (FEATURES_VOL), sans symbole, entraînée sur juillet à septembre (trou d'un jour). Horizons : 5 et 60 min. Succès : R² et Spearman supérieurs à la persistance calibrée sur le bloc de test, par symbole et par jour. Direction : modèles de la validation, résultat nul attendu. Essais sur la validation : 2 horizons × (5 modèles en direction, 4 en volatilité, 3 variantes de symbole), plus l'ajout des features de contexte.
---
Comment lire le test

Succès : R² et Spearman de la forêt au-dessus de la persistance, sur les trois symboles et presque chaque jour. Une baisse modérée par rapport à la validation est normale, car on a choisi les features sur celle-ci.
Échec : si la forêt tombe au niveau de la persistance, ou si le Spearman devient négatif certains jours, c'est le résultat honnête. Tu l'écris tel quel, avec les chiffres de validation à côté.
Dans les deux cas, aucun changement de modèle après ce résultat. Sinon le test devient une deuxième validation.
---
**Stabilité dans le temps.** À 5 min, la corrélation reste entre 0,20 et 0,38 chaque jour. À 60 min, elle reste positive tous les jours, mais elle chute vers 0,03 le 23/10 et vers 0,10 les 29 et 30/10. Les cibles à 60 min se chevauchent beaucoup, donc la mesure journalière est plus bruitée. Décris ces creux dans le rapport au lieu de les cacher.