# =========================================
# 🚀 Programa: Buscar menciones a @BancoPichincha y registrar en BigQuery con logs nativos de GitHub Actions
# =========================================

# 📦 Importar librerías
import tweepy
import pandas as pd
import logging
import json
import os
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

# =========================================
# ⚙️ Configuración de logs (stdout → GitHub Actions)
# =========================================
class JsonFormatter(logging.Formatter):
    def format(self, record):
        created_ec = datetime.now(timezone.utc).astimezone(
            timezone(timedelta(hours=-5))
        )
        log_record = {
            "timestamp": created_ec.replace(microsecond=0).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        return json.dumps(log_record)

logger = logging.getLogger("TwitterBigQueryLogger")
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(JsonFormatter())
logger.addHandler(stream_handler)

# =========================================
# 🔑 Autenticación Google BigQuery con Service Account (desde secret)
# =========================================
try:
    credentials_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not credentials_json:
        raise ValueError("❌ No se encontró GOOGLE_APPLICATION_CREDENTIALS_JSON en los secrets.")

    service_account_info = json.loads(credentials_json)
    client = bigquery.Client.from_service_account_info(service_account_info)

except Exception as e:
    logger.error(f"Error al inicializar BigQuery: {e}")
    raise

# =========================================
# 🔐 Función para cargar token desde secret
# =========================================
def cargar_token():
    try:
        token_env = os.environ.get("XTOKEN")
        if not token_env:
            raise ValueError("❌ XTOKEN no encontrado en los secrets.")

        for line in token_env.splitlines():
            if line.startswith("BEARER_TOKEN="):
                return line.strip().split("=", 1)[1]

        raise ValueError("❌ BEARER_TOKEN no encontrado dentro de XTOKEN.")

    except Exception as e:
        logger.error(f"Error al obtener el token: {e}")
        raise

# =========================================
# 🔎 Función para buscar tweets
# =========================================
def buscar_tweets(client):
    try:
        end_time = datetime.now(timezone.utc) - timedelta(seconds=15)
        start_time = end_time - timedelta(hours=1)

        query = "@BancoPichincha -is:retweet"

        tweets = client.search_recent_tweets(
            query=query,
            start_time=start_time.isoformat(timespec="seconds"),
            end_time=end_time.isoformat(timespec="seconds"),
            max_results=100,
            tweet_fields=["id", "text", "created_at", "public_metrics"],
            expansions=["author_id"],
            user_fields=["username"]
        )

        if not tweets.data:
            logger.info("No se encontraron tweets en la última hora.")
            return None

        users = {u["id"]: u for u in tweets.includes["users"]}

        data = []
        for t in tweets.data:
            created_ec = pd.Timestamp(t.created_at).tz_convert("America/Guayaquil").tz_localize(None)
            data.append({
                "Id": str(t.id),
                "Text": t.text,
                "Autor": users[t.author_id].username if t.author_id in users else t.author_id,
                "Retweet": t.public_metrics.get("retweet_count", 0),
                "Reply": t.public_metrics.get("reply_count", 0),
                "Likes": t.public_metrics.get("like_count", 0),
                "Quote": t.public_metrics.get("quote_count", 0),
                "Bookmark": t.public_metrics.get("bookmark_count", 0),
                "Impression": t.public_metrics.get("impression_count", None),
                "Created": created_ec
            })

        logger.info(f"{len(data)} tweets obtenidos de Twitter.")
        return pd.DataFrame(data)

    except tweepy.TooManyRequests:
        logger.error("Límite de peticiones alcanzado en la API de Twitter.")
        return None
    except tweepy.TweepyException as e:
        logger.error(f"Error en la API de Twitter: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado en buscar_tweets: {e}")
        return None

# =========================================
# 🗄️ Validar existencia de tweets en BigQuery
# =========================================
def obtener_ids_existentes(table_id):
    query = f"""
        SELECT Id
        FROM `{table_id}`
        ORDER BY Created DESC
        LIMIT 100
    """
    try:
        df = client.query(query).to_dataframe()
        ids = set(df["Id"].astype(str))
        logger.info(f"Se recuperaron {len(ids)} IDs de BigQuery.")
        return ids
    except Exception as e:
        logger.error(f"No se pudo obtener registros de BigQuery: {e}")
        return set()

# =========================================
# 🚀 Programa principal
# =========================================
def main():
    try:
        bearer_token = cargar_token()
        tw_client = tweepy.Client(bearer_token=bearer_token)

        df = buscar_tweets(tw_client)
        if df is None or df.empty:
            logger.info("No hay tweets nuevos para procesar.")
            return

        table_id = "xpry-472917.xds.xtable"
        ids_existentes = obtener_ids_existentes(table_id)
        df_nuevo = df[~df["Id"].isin(ids_existentes)]

        if df_nuevo.empty:
            logger.info("Todos los tweets ya existen en la base de datos.")
            return

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df_nuevo, table_id, job_config=job_config)
        job.result()

        logger.info(f"{len(df_nuevo)} tweets cargados correctamente a {table_id}")

    except Exception as e:
        logger.error(f"Error crítico en main: {e}")

# =========================================
# ▶️ Ejecutar
# =========================================
if __name__ == "__main__":
    main()
