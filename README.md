## Load transactions into neo4j

To load transaction data, run:
```bash
python3 load_neo4j.py & # Will load tasks into redis
celery worker -A load_neo4j -b redis:// --loglevel=error # Will run celery with redis queue to complete loading
```
