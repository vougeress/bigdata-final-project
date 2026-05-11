# Final Superset Dashboard Plan

## Dashboard

Build one Apache Superset dashboard named:

`IEEE-CIS Fraud Risk: EDA to Spark ML`

The dashboard tells the project story from raw Stage II exploratory analytics to
Stage III distributed predictive modeling. The supervised business proxy is
`isFraud`, used as a customer fraud-risk/default-risk proxy because the IEEE-CIS
dataset does not include a native default or churn label.

## Superset data source

Run this repository command before creating the Superset datasets:

```sh
python3 scripts/create_stage4_superset_db.py
```

It creates:

- `output/stage4/superset_dashboard.sqlite`
- `output/stage4/superset_dashboard_manifest.json`
- `output/stage4/model_metrics_long.csv`
- `output/stage4/best_model_summary.csv`
- `output/stage4/feature_profile_summary.csv`

Recommended cluster/Superset path:

```sh
export TEAM_NAME=team20
export PGHOST=hadoop-04.uni.innopolis.ru
export PGDATABASE=team20_projectdb
export PGUSER=team20
export PGPASSWORD='your-password'
STAGE4_LOAD_POSTGRES=1 bash scripts/stage4.sh
```

This loads the dashboard datasets into PostgreSQL as `stage4_*` tables. In
Superset, use the existing PostgreSQL connection to `team20_projectdb`, then
create datasets for these tables:

- `stage4_q1_product_risk`
- `stage4_q2_amount_band_risk`
- `stage4_q3_card_risk`
- `stage4_q4_email_domain_risk`
- `stage4_q5_daily_risk`
- `stage4_model_evaluation`
- `stage4_model_metrics_long`
- `stage4_best_model_summary`
- `stage4_feature_importance_gbt`
- `stage4_feature_importance_rf`
- `stage4_feature_profile_summary`

Local fallback: register the SQLite file in Superset as database
`team20_stage4_dashboard`, then create datasets for these tables:

- `q1_product_risk`
- `q2_amount_band_risk`
- `q3_card_risk`
- `q4_email_domain_risk`
- `q5_daily_risk`
- `model_evaluation`
- `model_metrics_long`
- `best_model_summary`
- `feature_importance_gbt`
- `feature_importance_rf`
- `feature_profile_summary`

If the Superset deployment cannot access a local SQLite file, upload the source
CSV files from `output/` plus the three helper CSV files from `output/stage4/`
and use the same dataset/table names listed above.

## Section 1. Business framing

Add one markdown panel:

```md
## IEEE-CIS Fraud Risk Dashboard

This dashboard uses IEEE-CIS transaction data to explain fraud-risk behavior
from raw exploratory analytics through Spark ML modeling.

Pipeline: Stage I PostgreSQL/HDFS ingestion -> Stage II Hive/Spark SQL EDA ->
Stage III Spark ML predictive modeling.

Target proxy: isFraud.
```

## Section 2. Preprocessing-free data insights

1. Product category fraud rate
   - dataset: `stage4_q1_product_risk` or `q1_product_risk`
   - chart: bar chart
   - x: `productcd`
   - metric: average `fraud_rate`

2. Transaction amount band risk
   - dataset: `stage4_q2_amount_band_risk` or `q2_amount_band_risk`
   - chart: ordered bar chart
   - x: `amount_band`
   - metric: average `fraud_rate`

3. Card network/type risk
   - dataset: `stage4_q3_card_risk` or `q3_card_risk`
   - chart: grouped bar chart
   - x: `card4`
   - series/group: `card6`
   - metric: average `fraud_rate`

4. Email domain fraud pattern
   - dataset: `stage4_q4_email_domain_risk` or `q4_email_domain_risk`
   - chart: horizontal bar chart
   - y/category: `email_domain`
   - metric: average `fraud_rate`

5. Daily fraud activity over time
   - dataset: `stage4_q5_daily_risk` or `q5_daily_risk`
   - chart: line chart
   - x: `transaction_day`
   - metric: average `fraud_rate`

6. Daily transaction volume over time
   - dataset: `stage4_q5_daily_risk` or `q5_daily_risk`
   - chart: line chart
   - x: `transaction_day`
   - metric: sum `total_transactions`

These six charts satisfy the requirement to show multiple data characteristics
before preprocessing.

## Section 3. Predictive modeling results

7. Model comparison
   - dataset: `stage4_model_metrics_long` or `model_metrics_long`
   - chart: grouped bar chart
   - x: `metric_name`
   - series/group: `model_name`
   - metric: average `metric_value`
   - format: 3 decimals

8. Best model highlight
   - dataset: `stage4_best_model_summary` or `best_model_summary`
   - chart: Big Number or markdown panel
   - value: `best_metric_value`
   - subtitle: `Best model: gbt, test AUC-PR 0.931`

9. GBT feature importance
   - dataset: `stage4_feature_importance_gbt` or `feature_importance_gbt`
   - chart: horizontal bar chart
   - y/category: `feature_name`
   - metric: average `importance`
   - filter/sort: top 20 by `importance`

10. RF feature importance
    - dataset: `stage4_feature_importance_rf` or `feature_importance_rf`
    - chart: horizontal bar chart
    - y/category: `feature_name`
    - metric: average `importance`
    - filter/sort: top 20 by `importance`

11. Feature filtering summary
    - dataset: `stage4_feature_profile_summary` or `feature_profile_summary`
    - chart: bar chart
    - x: `inclusion_decision`
    - metric: sum `feature_count`

## Presentation narration

Use this order during the defense:

1. Fraud is not random across product categories.
2. Transaction amount changes risk materially.
3. Payment card metadata carries meaningful signal.
4. Some email-domain groups are much riskier than others.
5. Fraud intensity and transaction volume vary over time.
6. Those raw patterns motivate the Stage III feature set.
7. The two Spark ML models were trained and tuned distributively.
8. `GBTClassifier` outperformed `RandomForestClassifier` on every reported
   final metric.
9. The most influential model features are amount-, card-, temporal-, and
   transaction-pattern related.

## Styling

- Use blue and gray for EDA charts.
- Use green for `gbt` and orange for `rf` in model comparison charts.
- Reserve red accents for fraud-rate emphasis.
- Format AUC, F1, and accuracy to 3 decimal places.
