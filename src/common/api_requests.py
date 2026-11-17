import time
import requests


def fetch_json(url: str, get=requests.get, sleep=time.sleep,
               max_retries: int = 5, wait_seconds: int = 60) -> str:
    """Llama a una API y devuelve el JSON como texto.
    Si responde 429 (límite de llamadas) o un error del servidor,
    espera y reintenta. Si se agotan los reintentos, lanza un error."""
    for attempt in range(1, max_retries + 1):
        response = get(url, timeout=60)
        if response.status_code == 200:
            return response.text
        if response.status_code == 429 or response.status_code >= 500:
            sleep(wait_seconds * attempt)
            continue
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
    raise RuntimeError(f"Se agotaron {max_retries} reintentos para {url}")