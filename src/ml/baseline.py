import numpy as np
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

TARGET = "is_delayed"

# airline (14) y origen (50) entran como categorías. El destino tiene más de 255 valores,
# el máximo que acepta el modelo como categoría, así que entra como código numérico.
CATEGORICAL = ["airline", "origin_airport"]
CODED = ["destination_airport"]

NUMERIC = ["distance", "planned_duration", "month", "day_of_month", "day_of_week", "is_weekend",
           "departure_hour_local", "arrival_hour_local", "is_holiday", "days_to_nearest_holiday",
           "temperature_c", "precipitation_mm", "snowfall_cm", "wind_speed_kmh", "wind_gusts_kmh",
           "cloud_cover_pct",
           "airline_flights_30d", "airline_delay_rate_30d", "origin_flights_30d", "origin_delay_rate_30d",
           "route_flights_30d", "route_delay_rate_30d"]

FEATURES = CATEGORICAL + CODED + NUMERIC


def fit_categories(train):
    """Los valores de cada columna de texto que aparecen en train (el modelo solo conoce esos)."""
    categories = {}
    for column in CATEGORICAL + CODED:
        categories[column] = sorted(train[column].dropna().unique())
    return categories


def prepare_features(df, categories):
    """Pasa todas las predictoras a números. Un valor de texto que no estaba en train queda vacío."""
    X = df[FEATURES].copy()
    for column in CATEGORICAL + CODED:
        codes = X[column].astype("category").cat.set_categories(categories[column]).cat.codes
        X[column] = codes.replace(-1, np.nan)
    for column in NUMERIC:
        X[column] = X[column].astype("float64")
    return X


def categorical_mask():
    """Qué columnas de FEATURES son categorías, para pasarle al modelo."""
    mask = []
    for column in FEATURES:
        mask.append(column in CATEGORICAL)
    return mask


def evaluate(y_true, probabilities, threshold=0.5):
    """AUC y, para la clase demorada, precision, recall y F1 con el umbral dado."""
    predicted = (probabilities >= threshold).astype(int)
    return {
        "auc": roc_auc_score(y_true, probabilities),
        "precision": precision_score(y_true, predicted, zero_division=0),
        "recall": recall_score(y_true, predicted, zero_division=0),
        "f1": f1_score(y_true, predicted, zero_division=0),
    }
