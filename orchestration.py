from datetime import datetime, timezone

from dagster import Definitions, Failure, ScheduleDefinition, job, op

from bridge import query


@op
def verifier_fraicheur(context):
    timestamp = query("SELECT max(event_time) FROM quotes")["dataset"][0][0]
    if timestamp is None:
        raise Failure("QuestDB ne contient aucune cotation.")
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(timestamp)).total_seconds()
    context.log.info(f"Dernière cotation reçue il y a {age:.0f} secondes.")
    if age > 120:
        raise Failure("La dernière cotation remonte à plus de deux minutes.")


@job
def controle_cotations():
    verifier_fraicheur()


defs = Definitions(
    jobs=[controle_cotations],
    schedules=[ScheduleDefinition(job=controle_cotations, cron_schedule="* * * * *")],
)
