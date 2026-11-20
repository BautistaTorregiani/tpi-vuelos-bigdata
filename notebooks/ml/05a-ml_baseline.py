# Databricks notebook source
# Modelo base (opcional). Correr en un cluster single node con runtime ML, o en serverless con
# %pip install scikit-learn si la cuota de la cuenta de estudiante no deja crear el cluster.

dbutils.widgets.text("catalog", "vuelos_dev")
dbutils.widgets.text("train_sample_rows", "1000000")
dbutils.widgets.text("seed", "42")
catalog = dbutils.widgets.get("catalog")
train_sample_rows = int(dbutils.widgets.get("train_sample_rows"))
seed = int(dbutils.widgets.get("seed"))

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import HistGradientBoostingClassifier
from src.ml.baseline import (TARGET, FEATURES, fit_categories, prepare_features,
                             categorical_mask, evaluate)

table = spark.table(f"{catalog}.gold.ml_flight_delay_training").select(FEATURES + [TARGET, "split"])

# Train es muy grande para un solo nodo: tomamos una muestra al azar DENTRO de train.
# Validation y test se usan enteros.
train_rows = table.filter("split = 'train'").count()
fraction = min(1.0, train_sample_rows / train_rows)
train = table.filter("split = 'train'").sample(fraction=fraction, seed=seed).toPandas()
validation = table.filter("split = 'validation'").toPandas()
test = table.filter("split = 'test'").toPandas()
print(f"train: {len(train):,} de {train_rows:,} | validation: {len(validation):,} | test: {len(test):,}")

categories = fit_categories(train)
X_train, y_train = prepare_features(train, categories), train[TARGET]
X_validation, y_validation = prepare_features(validation, categories), validation[TARGET]
X_test, y_test = prepare_features(test, categories), test[TARGET]

# COMMAND ----------

# Los experimentos van a la carpeta del usuario (los Git folders no pueden guardar experimentos)
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/tpi-vuelos-ml")
mlflow.sklearn.autolog()


def log_results(model, name):
    """Registra las métricas de validation y test con el prefijo de cada partición."""
    results = {}
    for split, X, y in [("validation", X_validation, y_validation), ("test", X_test, y_test)]:
        metrics = evaluate(y, model.predict_proba(X)[:, 1])
        for metric, value in metrics.items():
            results[f"{split}_{metric}"] = value
    mlflow.log_metrics(results)
    mlflow.log_param("train_rows", len(X_train))
    print(name, {k: round(v, 3) for k, v in results.items()})


# Línea base: predice para todos la tasa de demora de train (AUC = 0,5)
with mlflow.start_run(run_name="baseline_prior"):
    dummy = DummyClassifier(strategy="prior").fit(X_train, y_train)
    log_results(dummy, "baseline_prior")

# Modelo: gradient boosting, con pesos por clase por el desbalance (~1 demorado cada 5)
with mlflow.start_run(run_name="hist_gradient_boosting"):
    model = HistGradientBoostingClassifier(categorical_features=categorical_mask(),
                                           class_weight="balanced", max_iter=300,
                                           early_stopping=True, random_state=seed)
    model.fit(X_train, y_train)
    log_results(model, "hist_gradient_boosting")
