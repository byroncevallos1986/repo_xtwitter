# validate_secrets.py
import os
import json
import sys

def validate_secrets():
    """
    Valida la lectura de Repository Secrets en GitHub.
    - BEARER_TOKEN_1: Debe ser un string no vacío.
    - BEARER_TOKEN_2: Debe ser un string no vacío.
    - GOOGLE_APPLICATION_CREDENTIALS_JSON: Debe ser un JSON de service_account válido (no vacío).
    """
    results = {
        'BEARER_TOKEN_1': False,
        'BEARER_TOKEN_2': False,
        'GOOGLE_APPLICATION_CREDENTIALS_JSON': False,
        'BIGQUERY_TEST': False,
        'status': 'FAILED'
    }

    # Verificar BEARER_TOKEN_1
    token1 = os.getenv('BEARER_TOKEN_1')
    if token1 and len(token1.strip()) > 0:
        results['BEARER_TOKEN_1'] = True
        print(f"✅ BEARER_TOKEN_1: Cargado exitosamente (longitud: {len(token1)} caracteres)")
    else:
        print("❌ BEARER_TOKEN_1: No se pudo leer o está vacío.")

    # Verificar BEARER_TOKEN_2
    token2 = os.getenv('BEARER_TOKEN_2')
    if token2 and len(token2.strip()) > 0:
        results['BEARER_TOKEN_2'] = True
        print(f"✅ BEARER_TOKEN_2: Cargado exitosamente (longitud: {len(token2)} caracteres)")
    else:
        print("❌ BEARER_TOKEN_2: No se pudo leer o está vacío.")

    # Verificar GOOGLE_APPLICATION_CREDENTIALS_JSON
    json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if json_str and len(json_str.strip()) > 0:
        try:
            json_data = json.loads(json_str)
            if isinstance(json_data, dict) and json_data.get('type') == 'service_account':
                results['GOOGLE_APPLICATION_CREDENTIALS_JSON'] = True
                print("✅ GOOGLE_APPLICATION_CREDENTIALS_JSON: Cargado y válido (service_account detectado)")
            else:
                print("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON: JSON cargado pero estructura inválida (no es service_account).")
        except json.JSONDecodeError as e:
            print(f"❌ GOOGLE_APPLICATION_CREDENTIALS_JSON: JSON inválido. Error: {e}")
    else:
        print("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON: No se pudo leer o está vacío.")

    return results

def test_bigquery(table_fqn: str) -> bool:
    """
    Prueba de conexión a BigQuery ejecutando SELECT COUNT(*) sobre la tabla dada.
    Requiere que GOOGLE_APPLICATION_CREDENTIALS apunte a un archivo de credenciales válido.
    """
    try:
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound, Forbidden

        # Instanciar cliente (usa GOOGLE_APPLICATION_CREDENTIALS si está seteado)
        client = bigquery.Client()

        query = f"SELECT COUNT(*) AS total FROM `{table_fqn}`"
        print(f"🔎 Ejecutando consulta de prueba a BigQuery: {query}")
        job = client.query(query)
        result = list(job.result())  # materializa resultados
        if result:
            total = result[0].get("total")
            print(f"✅ BigQuery OK: La tabla `{table_fqn}` respondió COUNT(*) = {total}")
            return True
        else:
            print("⚠️ BigQuery respondió sin filas para la consulta de prueba.")
            return False

    except NotFound:
        print(f"❌ BigQuery: La tabla `{table_fqn}` no existe o el dataset/proyecto es incorrecto.")
        return False
    except Forbidden as e:
        print(f"❌ BigQuery: Permisos insuficientes para acceder a `{table_fqn}`. Detalle: {e}")
        return False
    except Exception as e:
        print(f"❌ BigQuery: Error inesperado al consultar `{table_fqn}`. Detalle: {e}")
        return False

if __name__ == "__main__":
    results = validate_secrets()

    # Si el JSON de credenciales de servicio es válido, intentamos la prueba de BigQuery.
    # Nota: el workflow escribirá el JSON a un archivo y seteará GOOGLE_APPLICATION_CREDENTIALS.
    TABLE_FQN = os.getenv("BQ_TABLE_FQN", "xpry-472917.xds.xtable")
    if results.get('GOOGLE_APPLICATION_CREDENTIALS_JSON'):
        bq_ok = test_bigquery(TABLE_FQN)
        results['BIGQUERY_TEST'] = bq_ok
    else:
        print("⏭️ Omitiendo prueba de BigQuery: credenciales de servicio no válidas.")

    if (results['BEARER_TOKEN_1']
        and results['BEARER_TOKEN_2']
        and results['GOOGLE_APPLICATION_CREDENTIALS_JSON']
        and results['BIGQUERY_TEST']):
        results['status'] = 'SUCCESS'
        print("\n🎉 ¡Todos los checks pasaron (secrets y BigQuery)!")
        sys.exit(0)
    else:
        print("\n⚠️ Algunos checks fallaron. Revisa los logs arriba y la configuración de Secrets/Permisos.")
        sys.exit(1)
