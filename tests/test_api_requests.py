import pytest
import requests
from src.common.api_requests import fetch_json


class FakeResponse:
    """Una respuesta inventada, como la que devolvería la API."""

    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


class FakeAPI:
    """Una API falsa: responde con los códigos que le indiquemos, en orden,
    y cuenta cuántas veces la llamaron. Así no usamos internet."""

    def __init__(self, status_codes):
        self.status_codes = status_codes
        self.calls = 0

    def get(self, url, timeout):
        code = self.status_codes[self.calls]
        self.calls += 1
        if code == "timeout":
            raise requests.exceptions.Timeout("Read timed out")
        if code == 200:
            return FakeResponse(200, '{"ok": true}')
        return FakeResponse(code)


def no_wait(seconds):
    """Reemplaza la espera real, para que el test no tarde minutos."""
    pass


def test_retries_after_429_and_returns_json():
    # Preparar: la API falla con 429 la primera vez y responde bien la segunda
    api = FakeAPI(status_codes=[429, 200])

    # Ejecutar
    result = fetch_json("url", get=api.get, sleep=no_wait)

    # Verificar: devolvió el JSON y llamó a la API dos veces
    assert result == '{"ok": true}'
    assert api.calls == 2


def test_fails_after_max_retries():
    # Preparar: la API responde 429 siempre
    api = FakeAPI(status_codes=[429, 429, 429])

    # Ejecutar y verificar: con 3 intentos como máximo, se rinde y lanza un error
    with pytest.raises(RuntimeError):
        fetch_json("url", get=api.get, sleep=no_wait, max_retries=3)

    assert api.calls == 3


def test_retries_after_timeout():
    # Preparar: la conexión se corta la primera vez y la segunda responde bien
    api = FakeAPI(status_codes=["timeout", 200])

    # Ejecutar
    result = fetch_json("url", get=api.get, sleep=no_wait)

    # Verificar: devolvió el JSON y llamó a la API dos veces
    assert result == '{"ok": true}'
    assert api.calls == 2