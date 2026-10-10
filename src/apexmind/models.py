from dataclasses import dataclass
import numpy as np
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor
from sklearn.feature_selection import f_regression
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.preprocessing import StandardScaler
from .features import FEATURES

MODELS = (
    "reference_momentum",
    "cross_venue_lag",
    "order_flow",
    "lag_and_flow",
    "regularized",
    "gradient_boosted",
)
DIRECTIONAL = {
    "basis_bps",
    "basis_z",
    "lag_250ms",
    "lag_1s",
    "lag_5s",
    "reference_250ms",
    "reference_1s",
    "reference_5s",
    "lighter_250ms",
    "lighter_1s",
    "lighter_5s",
    "imbalance",
    "flow_imbalance",
    "aggressive_ratio",
}


def directed(x):
    out = x.copy()
    for i, name in enumerate(FEATURES):
        if name in DIRECTIONAL:
            out[:, i] *= x[:, -1]
    return out


def block_mean_ci(values, seed=42, samples=500, alpha=0.05):
    values = np.asarray(values, dtype=float)
    if len(values) < 2:
        return [None, None]
    rng = np.random.default_rng(seed)
    block = max(2, int(np.sqrt(len(values))))
    starts = np.arange(max(1, len(values) - block + 1))
    means = []
    for _ in range(samples):
        indices = np.concatenate(
            [
                np.arange(s, min(s + block, len(values)))
                for s in rng.choice(starts, int(np.ceil(len(values) / block)))
            ]
        )
        means.append(float(np.mean(values[indices[: len(values)]])))
    return [float(np.quantile(means, alpha / 2)), float(np.quantile(means, 1 - alpha / 2))]


@dataclass
class ForecastModel:
    name: str
    seed: int = 42
    max_features: int = 12
    slippage_bps: float = 5

    def fit(self, x, targets):
        all_x = directed(x)
        simple = {
            "reference_momentum": ["reference_250ms", "reference_1s", "reference_5s"],
            "cross_venue_lag": ["basis_bps", "lag_250ms", "lag_1s", "lag_5s"],
            "order_flow": ["imbalance", "flow_imbalance", "aggressive_ratio"],
            "lag_and_flow": [
                "basis_bps",
                "lag_250ms",
                "lag_1s",
                "lag_5s",
                "imbalance",
                "flow_imbalance",
                "aggressive_ratio",
            ],
        }
        if self.name in simple:
            selected = [FEATURES.index(k) for k in simple[self.name]] + [len(FEATURES)]
        else:
            variable = np.flatnonzero(np.std(all_x, axis=0) > 1e-12)
            if not len(variable):
                raise ValueError("No nonconstant training features")
            corr = np.atleast_2d(np.corrcoef(all_x[:, variable], rowvar=False))
            keep = []
            for i in range(len(variable)):
                if all(abs(corr[i, j]) < 0.98 for j in keep):
                    keep.append(i)
            selected = variable[keep]
            score, _ = f_regression(all_x[:, selected], targets["net_bps"])
            selected = selected[np.argsort(np.nan_to_num(score))[-self.max_features :]]
        self.selected = np.array(selected, dtype=int)
        self.scaler = StandardScaler().fit(all_x[:, self.selected])
        z = self.scaler.transform(all_x[:, self.selected])

        def regression(y):
            estimator = (
                HistGradientBoostingRegressor(
                    max_iter=80, max_leaf_nodes=15, l2_regularization=10, random_state=self.seed
                )
                if self.name == "gradient_boosted"
                else Ridge(alpha=10)
            )
            return estimator.fit(z, y)

        def classification(y):
            estimator = (
                LogisticRegression(C=0.1, max_iter=500, random_state=self.seed)
                if len(np.unique(y)) > 1
                else DummyClassifier(strategy="prior")
            )
            return estimator.fit(z, y)

        self.mean_model = regression(targets["net_bps"])
        self.adverse_model = regression(targets["mae_bps"])
        self.probability_model = classification(targets["net_bps"] > 0)
        self.slippage_model = classification(targets["entry_slippage_bps"] > self.slippage_bps)
        self.latency_model = regression(targets["entry_delay_ms"])
        self.holding_model = regression(targets["holding_seconds"])
        self.quantile_models = {}
        if self.name == "gradient_boosted":
            for q in (0.05, 0.5, 0.95):
                self.quantile_models[q] = GradientBoostingRegressor(
                    loss="quantile",
                    alpha=q,
                    n_estimators=50,
                    max_depth=2,
                    min_samples_leaf=20,
                    random_state=self.seed,
                ).fit(z, targets["net_bps"])
        self.residual_quantiles = np.zeros(3)
        self.mean_correction = 0.0
        self.threshold = float("inf")
        self.trained_until_wall_ns = int(targets["label_end_wall_ns"].max())
        return self

    def calibrate(self, x, y, bootstrap_samples=500):
        pred = self.predict(x)["mean"]
        residual = y - pred
        self.residual_quantiles = np.quantile(residual, [0.05, 0.5, 0.95])
        # Calibration is chronological and separate from the held-out period.
        ci = block_mean_ci(residual, self.seed, bootstrap_samples, alpha=0.05 / len(MODELS))
        self.mean_correction = min(0.0, ci[0]) if ci[0] is not None else -float("inf")

    def predict(self, x):
        z = self.scaler.transform(directed(x)[:, self.selected])
        mean = self.mean_model.predict(z)

        def positive(model):
            classes = list(model.classes_)
            return (
                model.predict_proba(z)[:, classes.index(True)]
                if True in classes
                else np.zeros(len(z))
            )

        if self.quantile_models:
            quantiles = np.sort(
                np.stack([self.quantile_models[q].predict(z) for q in (0.05, 0.5, 0.95)], axis=1),
                axis=1,
            )
        else:
            quantiles = mean[:, None] + self.residual_quantiles
        return {
            "mean": mean,
            "lower_mean": mean + self.mean_correction,
            "q05": quantiles[:, 0],
            "q50": quantiles[:, 1],
            "q95": quantiles[:, 2],
            "favorable_probability": positive(self.probability_model),
            "adverse": np.maximum(0, self.adverse_model.predict(z)),
            "slippage_probability": positive(self.slippage_model),
            "execution_delay_ms": np.maximum(0, self.latency_model.predict(z)),
            "holding_seconds": np.maximum(0.01, self.holding_model.predict(z)),
        }

    def selected_names(self):
        return [(FEATURES + ["side"])[i] for i in self.selected]
