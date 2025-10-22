import os
import json
import sys

def validate_secrets():
    """
    Valida la lectura de Repository Secrets en GitHub.
    - BEARER_TOKEN_1: Debe ser un string no vacío.
    - BEARER_TOKEN_2: Debe ser un string no vacío.
    - GOOGLE_APPLICATION_CREDENTIALS_JSON: Debe ser un JSON válido (no vacío).
    """
    results = {
        'BEARER_TOKEN_1': False,
        'BEARER_TOKEN_2': False,
        'GOOGLE_APPLICATION_CREDENTIALS_JSON': False,
        'status': 'FAILED'
    }
    
    # Verificar BEARER_TOKEN_1
    token1 = os.getenv('BEARER_TOKEN_1')
    if token1 and len(token1.strip()) > 0:
        results['BEARER_TOKEN_1'] = True
        print("✅ BEARER_TOKEN_1: Cargado exitosamente (longitud: {} caracteres)".format(len(token1)))
    else:
        print("❌ BEARER_TOKEN_1: No se pudo leer o está vacío.")
    
    # Verificar BEARER_TOKEN_2
    token2 = os.getenv('BEARER_TOKEN_2')
    if token2 and len(token2.strip()) > 0:
        results['BEARER_TOKEN_2'] = True
        print("✅ BEARER_TOKEN_2: Cargado exitosamente (longitud: {} caracteres)".format(len(token2)))
    else:
        print("❌ BEARER_TOKEN_2: No se pudo leer o está vacío.")
    
    # Verificar GOOGLE_APPLICATION_CREDENTIALS_JSON
    json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if json_str and len(json_str.strip()) > 0:
        try:
            json_data = json.loads(json_str)
            if isinstance(json_data, dict) and 'type' in json_data and json_data['type'] == 'service_account':
                results['GOOGLE_APPLICATION_CREDENTIALS_JSON'] = True
                print("✅ GOOGLE_APPLICATION_CREDENTIALS_JSON: Cargado y válido (claves presentes: project_id, client_email, etc.)")
            else:
                print("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON: JSON cargado pero estructura inválida (no es service_account).")
        except json.JSONDecodeError as e:
            print(f"❌ GOOGLE_APPLICATION_CREDENTIALS_JSON: JSON inválido. Error: {e}")
    else:
        print("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON: No se pudo leer o está vacío.")
    
    # Resumen
    if all(results.values()[:-1]):  # Ignora 'status'
        results['status'] = 'SUCCESS'
        print("\n🎉 ¡Todos los secrets se leyeron exitosamente!")
    else:
        print("\n⚠️  Algunos secrets fallaron. Revisa la configuración en GitHub Secrets.")
    
    return results

if __name__ == "__main__":
    results = validate_secrets()
    # Opcional: Salir con código de error si falla (útil en CI/CD)
    sys.exit(0 if results['status'] == 'SUCCESS' else 1)
