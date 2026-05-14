# Text-Only Baseline: External Near-Miss v7

Offline diagnostic over frozen external near-miss embeddings and v7 labels only.

## Inputs

- **embeddings:** `docs/audit/ml-external-text-embeddings-v7.json`
- **embeddings_sha256:** `92227939f84e158a3110998903bf077ee416b139af6aabc23b9c07163ec97ac3`
- **label_dataset:** `docs/audit/ml-label-dataset-v7.json`
- **label_dataset_sha256:** `094af1a6083561803c26611e1d6f0afebba6eedec0d2e9ac21008f415117dc85`
- **review_pool_variant:** `ml_external_near_miss_audit`
- **joined rows:** `60`
- **random_seed:** `0`

## Model Comparison

| Target | Model | Accuracy | Balanced accuracy | Macro F1 | ROC-AUC | TN | FP | FN | TP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| good_or_acceptable class counts | pos=17 neg=43 null=0 folds=5 |  |  |  |  |  |  |  |  |
| good_or_acceptable | embedding_logistic | 0.850 | 0.771 | 0.795 | 0.944 | 41 | 2 | 7 | 10 |
| good_or_acceptable | metadata_sample_reason_logistic | 0.683 | 0.477 | 0.406 | 0.617 | 41 | 2 | 17 | 0 |
| good_or_acceptable | majority_class | 0.717 | 0.500 | 0.417 | null | 43 | 0 | 17 | 0 |
| good_or_acceptable | stratified_random_prevalence | 0.567 | 0.484 | 0.484 | null | 29 | 14 | 12 | 5 |
| surprising_or_useful class counts | pos=38 neg=22 null=0 folds=5 |  |  |  |  |  |  |  |  |
| surprising_or_useful | embedding_logistic | 0.767 | 0.730 | 0.738 | 0.839 | 13 | 9 | 5 | 33 |
| surprising_or_useful | metadata_sample_reason_logistic | 0.750 | 0.678 | 0.687 | 0.725 | 9 | 13 | 2 | 36 |
| surprising_or_useful | majority_class | 0.633 | 0.500 | 0.388 | null | 0 | 22 | 0 | 38 |
| surprising_or_useful | stratified_random_prevalence | 0.517 | 0.494 | 0.493 | null | 9 | 13 | 16 | 22 |

## Not A Production Recommender Test

This is not a production recommender test. Production-grade evaluation would still require deliberate splits, larger and multi-reviewer labels, product-matched candidate pools, top-k workflow metrics, and shadow or flagged experiments.

## Caveats

- Not validation.
- Single-reviewer audit labels.
- External near-miss pool only.
- No product ranking or API behavior change.
- Frozen offline embeddings only.
- No claim of live recommender quality.
- CV leakage guard applies only to the described folds and preprocessing, not to broader sampling bias.
