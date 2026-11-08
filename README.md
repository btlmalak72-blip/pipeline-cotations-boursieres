# Démo de cotations boursières

Le simulateur publie des cotations fictives pour AAPL, MSFT, NVDA et TSLA dans le topic Redpanda `quotes.raw`. Chaque message JSON contient le symbole, le prix et l’heure UTC de la cotation. La clé du message est le symbole.

Ouvre Docker Desktop avec le moteur Linux, puis ouvre PowerShell dans le dossier qui contient ce fichier. Python 3 suffit pour le simulateur. Il utilise le client `rpk` fourni par le conteneur.

## Lancer un premier essai

```powershell
docker compose up -d --wait redpanda
docker compose exec -T redpanda rpk topic create quotes.raw -X brokers=redpanda:9092
python .\simulateur.py --count 3
docker compose exec -T redpanda rpk topic consume quotes.raw -n 3 -o start -f '%v\n' -X brokers=redpanda:9092
```

Crée le topic au premier lancement. Le volume Docker conserve les messages et le topic entre les redémarrages. La dernière commande lit les trois premiers messages enregistrés.

## Voir les nouvelles cotations en direct

Dans un premier terminal PowerShell, lance la lecture des nouveaux messages.

```powershell
docker compose exec -T redpanda rpk topic consume quotes.raw -o end -f '%v\n' -X brokers=redpanda:9092
```

Dans un second terminal, ouvert dans le même dossier, démarre le simulateur.

```powershell
python .\simulateur.py
```

Arrête chaque commande avec Ctrl+C, puis lance `docker compose down` pour arrêter le conteneur.

[Documentation Redpanda pour démarrer un broker local](https://docs.redpanda.com/labs/docker-compose/single-broker/)

## Stocker les prix et ouvrir Grafana

```powershell
docker compose up -d --build --wait
docker compose exec -T redpanda rpk topic list -X brokers=redpanda:9092
docker compose exec -T redpanda rpk topic create quotes.raw -p 1 -X brokers=redpanda:9092
docker compose exec -T redpanda rpk topic create quotes.minute -p 1 -X brokers=redpanda:9092
python .\simulateur.py
```

Exécute les commandes de création uniquement pour les topics absents de la liste. Laisse le simulateur tourner dans son terminal. Ouvre [Grafana](http://localhost:3100/d/cotations) avec `admin` / `admin` pour la première connexion. Le tableau « Cotations boursières » affiche une courbe par symbole sur les quinze dernières minutes. Tu peux également ouvrir [QuestDB](http://localhost:9100) et exécuter `SELECT * FROM quotes ORDER BY event_time DESC LIMIT 10;`. Si la table reste vide, consulte `docker compose logs bridge`.

## Calculer les moyennes par minute avec Flink et les lire dans Redis

Dans un second PowerShell ouvert dans le même dossier, télécharge le connecteur Kafka pour Flink et lance le calcul.

```powershell
Invoke-WebRequest 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar' -OutFile kafka.jar
docker compose --profile flink up -d jobmanager taskmanager
docker compose -f compose.yaml -f flink-client.yaml --profile flink run --rm sql-client
docker compose exec -T redis redis-cli XREVRANGE quotes:minute + - COUNT 3
```

Laisse passer au moins une minute avec le simulateur actif et relance la commande Redis. Flink clôture une fenêtre quand l’heure des cotations a dépassé la fin de la fenêtre de cinq secondes. Chaque résultat contient la moyenne et le nombre de cotations. Le [tableau Flink](http://localhost:8281) indique si le calcul tourne.

## Vérifier la fraîcheur avec Dagster

```powershell
docker compose --profile dagster up -d dagster
```

Dans [Dagster](http://localhost:3101), lance le job `controle_cotations` ou active sa planification pour une exécution chaque minute. Arrête le simulateur et attends deux minutes après la dernière cotation pour observer un échec.

## Utiliser des transactions réelles

Arrête le simulateur avant de démarrer le collecteur, pour séparer les sources pendant la démo. Le topic et la table conservent l’historique simulé. Le collecteur utilise les transactions AAPL de Massive. Une clé autorisée pour le WebSocket actions est nécessaire. Saisis la clé dans ton terminal sans l’inscrire dans le projet.

```powershell
$env:MASSIVE_API_KEY = Read-Host 'Clé Massive' -MaskInput
docker compose --profile live run --rm websocket
```

Pour arrêter la démo et conserver les données, exécute `docker compose --profile '*' down`.
