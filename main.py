# =========================================
# 🚀 main.py
# =========================================

import os
import tweepy
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

def get_twitter_client():
    """Obtiene el cliente de la API de XTwitter usando BEARER_TOKEN_1 o BEARER_TOKEN_2."""
    token1 = os.getenv("BEARER_TOKEN_1")
    token2 = os.getenv("BEARER_TOKEN_2")

    for token in [token1, token2]:
        if token:
            try:
                client = tweepy.Client(bearer_token=token, wait_on_rate_limit=True)
                # Probar una consulta simple
                client.get_user(username="BancoPichincha")
                print(f"✅ Conexión exitosa a la API de Twitter con token ({len(token)} caracteres)")
                return client
            except Exception as e:
                print(f"⚠️ Error con el token (len={len(token)}): {e}")
    raise RuntimeError("❌ No se pudo autenticar con ninguno de los BEARER_TOKEN disponibles.")


def fetch_tweets(client):
    """Obtiene tweets de las últimas 24 horas que mencionen a @BancoPichincha."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=24)
    query = "@BancoPichincha -is:retweet"

    print(f"🔎 Buscando tweets desde {start_time.isoformat()} hasta {end_time.isoformat()}...")

    tweets = []
    for tweet in tweepy.Paginator(
        client.search_recent_tweets,
        query=query,
        tweet_fields=["created_at", "public_metrics", "author_id", "text"],
        user_fields=["username"],
        expansions=["author_id"],
        max_results=100
    ).flatten(limit=200):
        tweets.append(tweet)

    print(f"✅ Se obtuvieron {len(tweets)} tweets recientes.")
    return tweets


def build_dataframe(tweets, client):
    """Transforma los tweets en un DataFrame compatible con BigQuery."""
    data = []
    users = {}

    # Obtener los usuarios en bloque (para reducir llamadas)
    author_ids = list({t.author_id for t in tweets if t.author_id})
    user_data = client.get_users(ids=author_ids, user_fields=["username"]).data or []
    for u in user_data:
        users[u.id] = u.username

    for t in tweets:
        metrics = t.public_metrics
        data.append({
            "Id": str(t.id),
            "Text": t.text,
            "Autor": users.get(t.author_id, "desconocido"),
            "Retweet": metrics.get("retweet_count", 0),
            "Reply": metrics.get("reply_count", 0),
            "Likes": metrics.get("like_count", 0),
            "Quote": metrics.get("quote_count", 0),
            "Bookmark": metrics.get("bookmark_count", 0),
            "Impression": metrics.get("impression_count", 0),
            "Created": t.created_at.astimezone(timezone(timedelta(hours=-5)))  # hora Ecuador
        })

    df = pd.DataFrame(data)
    print(f"📊 DataFrame creado con {len(df)} registros.")
    return df


def load_to_bigquery(df, table_fqn):
    """Carga los tweets en la tabla BigQuery."""
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("Id", "STRING"),
            bigquery.SchemaField("Text", "STRING"),
            bigquery.SchemaField("Autor", "STRING"),
            bigquery.SchemaField("Retweet", "INTEGER"),
            bigquery.SchemaField("Reply", "INTEGER"),
            bigquery.SchemaField("Likes", "INTEGER"),
            bigquery.SchemaField("Quote", "INTEGER"),
            bigquery.SchemaField("Bookmark", "INTEGER"),
            bigquery.SchemaField("Impression", "INTEGER"),
            bigquery.SchemaField("Created", "DATETIME"),
        ],
    )

    print(f"🚀 Cargando datos en BigQuery: {table_fqn} ...")
    job = client.load_table_from_dataframe(df, table_fqn, job_config=job_config)
    job.result()  # Esperar a que finalice
    print(f"✅ {len(df)} registros insertados en {table_fqn} exitosamente.")


if __name__ == "__main__":
    TABLE_FQN = os.getenv("BQ_TABLE_FQN", "xpry-472917.xds.xtable")

    twitter_client = get_twitter_client()
    tweets = fetch_tweets(twitter_client)

    if tweets:
        df = build_dataframe(tweets, twitter_client)
        load_to_bigquery(df, TABLE_FQN)
    else:
        print("⚠️ No se encontraron tweets en las últimas 24 horas.")
