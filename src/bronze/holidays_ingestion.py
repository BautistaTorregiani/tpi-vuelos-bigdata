BASE_URL = "https://date.nager.at/api/v3/PublicHolidays"


def build_holidays_url(year: int, country_code: str) -> str:
    """Arma la URL de Nager.Date para los feriados de un año y país."""
    return f"{BASE_URL}/{year}/{country_code}"