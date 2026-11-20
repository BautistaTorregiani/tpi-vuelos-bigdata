import numpy as np
import pandas as pd

from src.ml.baseline import FEATURES, categorical_mask, evaluate, fit_categories, prepare_features


def make_flights(rows):
    """Vuelos de prueba: cada fila es (aerolínea, origen, destino); el resto de las predictoras en 1."""
    df = pd.DataFrame(rows, columns=["airline", "origin_airport", "destination_airport"])
    for column in FEATURES[3:]:
        df[column] = 1
    df["is_weekend"] = True
    return df


def test_unknown_category_becomes_empty():
    train = make_flights([("AA", "ATL", "JFK"), ("DL", "ORD", "LAX")])
    test = make_flights([("ZZ", "ATL", "JFK")])          # ZZ no estaba en train
    X = prepare_features(test, fit_categories(train))
    assert np.isnan(X["airline"].iloc[0])
    assert X["origin_airport"].iloc[0] == 0               # ATL es el primero de la lista


def test_all_features_are_numbers():
    train = make_flights([("AA", "ATL", "JFK")])
    X = prepare_features(train, fit_categories(train))
    assert list(X.columns) == FEATURES
    assert X["is_weekend"].iloc[0] == 1.0                  # el booleano pasa a número


def test_only_airline_and_origin_are_categories():
    mask = categorical_mask()
    assert mask[FEATURES.index("airline")] and mask[FEATURES.index("origin_airport")]
    assert sum(mask) == 2


def test_perfect_predictions_have_auc_1():
    metrics = evaluate(np.array([0, 0, 1, 1]), np.array([0.1, 0.2, 0.8, 0.9]))
    assert metrics["auc"] == 1.0
    assert metrics["precision"] == 1.0 and metrics["recall"] == 1.0
