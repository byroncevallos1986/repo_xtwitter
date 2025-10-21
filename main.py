# =========================================
# 🚀 Programa: Buscar menciones a @BancoPichincha y registrar en BigQuery
#        (Logs compatibles con ELK y GitHub Actions)
# =========================================

# 📦 Importar librerías
import tweepy
import pandas as pd
import logging
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

# =========================================
# ⚙️ Configuración: rutas/config desde ENV (útil en GitHub Actions)
# =========================================
# Rutas por defecto (para ejecución local). En GitHub Actions se crearán los archivos
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH", "service_account.json")
token_paths = [
    os.getenv("XTOKEN1_PATH", "xtoken1.env"),
    os.getenv("XTOKEN2_PATH", "xtoken2.env")
]
log_file = os.getenv("XLOG_PATH", "xlog.log")

# =========================================
# 🔧 Formatters para logging
# - FileHandler: JSON (ELK)
# - StreamHandler: salida legible para GitHub Actions (stdout)
# =========================================
class JsonFormatter(logging.Formatter):
    def format(self, record):
        # Fecha en hora de Ecuador (UTC-5)
        created_ec = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
        log_record = {
            "timestamp": created_ec.replace(microsecond=0).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "logger": record.name,
        }
        # Añadir información extra si existe
        if getattr(record, "extra", None):
            log_record["extra"] = record.extra
        return json.dumps(log_record)

class ConsoleFormatter(logging.Formatter):
    def format(self, record):
        # Timestamp en UTC ISO para GitHub (fácil de leer)
        ts = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        return f"{ts} [{record.levelname}] {record.getMessage()} (module={record.module} func={record.funcName} line={record.lineno})"

# =========================================
# 🧰 Configurar logger principal
# =========================================
logger = logging.getLogger("TwitterBigQueryLogger")
logger.setLevel(logging.INFO)
logger.propagate = False  # evitar duplicación si root handler existe

# File handler (JSON) - para ELK / recolección
try:
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)
except Exception as e:
    # Si el archivo no se puede crear (por permisos), emitir advertencia en stdout
    print(f"WARNING: No se pudo crear archivo de log {log_file}: {e}", file=sys.stderr)

# Stream handler (stdout) - para GitHub Actions
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(ConsoleFormatter())
logger.addHandler(stream_handler)

# =========================================
# 🚩 Helpers para GitHub Actions grouping (opcional)
# =========================================
GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS", "false").lower() == "true"

def start_group(name: str):
    if GITHUB_ACTIONS:
        # Sintaxis especial de Actions para agrupar logs
        print(f"::group::{name}")
    else:
        logger.info(f"--- {name} ---")

def end_group():
    if GITHUB_ACTIONS:
        print("::endgroup::")
    else:
        logger.info("--- end ---")

# =========================================
# 🔑 Inicializar cliente BigQuery (usa service_account_path)
# =========================================
try:
    client = bigquery.Client.from_service_account_json(service_account_path,
                                                      project=os.getenv("GCP_PROJECT", None))
    logger.info("Cliente de BigQuery inicializado correctamente.")
except Exception as e:
    logger.error(f"No se pudo inicializar BigQuery con {service_account_path}: {e}")
    # Fallo crítico: lanzar excepción para que CI/GitHub Actions lo registre y marque job como fallido
    raise

# =========================================
# 🔐 Función para cargar token con soporte ENV (útil en Actions)
# =========================================
def cargar_token(path, env_override_name=None):
    """
    Lee token desde:
      1) variable de entorno env_override_name (si se proporciona)
      2) archivo .env con línea BEARER_TOKEN=...
    Retorna None si no se encuentra.
    """
    # 1) revisar variable de entorno (si se pasó)
    if env_override_name and os.getenv(env_override_name):
        return os.getenv(env_override_name)

    # 2) leer archivo
    try:
        if not os.path.exists(path):
            logger.warning(f"Archivo token no existe: {path}")
            return None
        with open(path, "r") as f:
            for line in f:
                if line.strip().startswith("BEARER_TOKEN="):
                    token = line.strip().split("=", 1)[1]
                    if token:
                        return token
        logger.warning(f"BEARER_TOKEN no encontrado en {path}")
        return None
    except Exception as e:
        logger.error(f"Error al leer el token {path}: {e}")
        return None

# =========================================
# 🔎 Función para buscar tweets de las últimas 24 horas
# =========================================
def buscar_tweets(client_twitter):
    try:
        end_time = datetime.now(timezone.utc) - timedelta(seconds=15)
        start_time = end_time - timedelta(hours=24)  # Últimas 24 horas

        query = "@BancoPichincha @superbancosEC -is:retweet"

        tweets = client_twitter.search_recent_tweets(
            query=query,
            start_time=start_time.isoformat(timespec="seconds"),
            end_time=end_time.isoformat(timespec="seconds"),
            max_results=100,
            tweet_fields=["id", "text", "created_at", "public_metrics"],
            expansions=["author_id"],
            user_fields=["username"]
        )

        if not tweets or not getattr(tweets, "data", None):
            logger.info("No se encontraron tweets en las últimas 24 horas.")
            return pd.DataFrame()

        users = {u["id"]: u for u in tweets.includes.get("users", [])}
        data = []
        for t in tweets.data:
            # convertir created_at a timezone America/Guayaquil y dejar naive (como antes)
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

        logger.info(f"{len(data)} tweets obtenidos de Twitter en las últimas 24 horas.")
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
        LIMIT 500
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
        start_group("Proceso principal: autenticación y búsqueda")

        # Intentar autenticación con ambos tokens (admite override por env: BEARER_TOKEN_1 / BEARER_TOKEN_2)
        for idx, path in enumerate(token_paths, start=1):
            env_name = f"BEARER_TOKEN_{idx}"
            bearer_token = cargar_token(path, env_override_name=env_name)
            if not bearer_token:
                logger.warning(f"No se pudo cargar el token (path={path}, env={env_name}). Se probará el siguiente.")
                continue

            logger.info(f"Usando Bearer Token #{idx}")
            try:
                tw_client = tweepy.Client(bearer_token=bearer_token)
                df = buscar_tweets(tw_client)

                if df is None:
                    # Error en API o límite → intentar siguiente token
                    logger.warning(f"Problema al consultar con Bearer Token #{idx}. Intentando con el siguiente...")
                    continue

                if df.empty:
                    logger.info("No hay tweets nuevos para procesar.")
                    end_group()
                    return

                # Validar duplicados con BigQuery
                table_id = os.getenv("BQ_TABLE_ID", "xpry-472917.xds.xtable")
                ids_existentes = obtener_ids_existentes(table_id)
                df_nuevo = df[~df["Id"].isin(ids_existentes)]

                if df_nuevo.empty:
                    logger.info("Todos los tweets ya existen en la base de datos.")
                    end_group()
                    return

                # Subir a BigQuery
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                job = client.load_table_from_dataframe(df_nuevo, table_id, job_config=job_config)
                job.result()

                logger.info(f"{len(df_nuevo)} tweets cargados correctamente a {table_id}")
                end_group()
                return  # Éxito → salir sin probar el siguiente token

            except Exception as e:
                logger.error(f"Error usando Bearer Token #{idx}: {e}")
                # continuar con siguiente token
                continue

        # Si ningún token funcionó
        logger.error("❌ Ninguno de los Bearer Tokens funcionó correctamente.")
        end_group()

    except Exception as e:
        logger.error(f"Error crítico en main: {e}")
        # Lanzar para que GitHub Actions marque el job como fallido (si así lo deseas)
        raise

# =========================================
# ▶️ Ejecutar
# =========================================
if __name__ == "__main__":
    main()
