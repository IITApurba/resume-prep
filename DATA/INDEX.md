# DATA: Project Interview Playbooks

Four concise playbooks (19–20 A4 pages each) for Data Science, ML Engineering and AI Engineering interviews of 30–45 minutes. Each one teaches a resume project from first principles and prepares you to defend every bullet.

| # | Project | What it covers | Source | PDF |
|---|---|---|---|---|
| 1 | **Risk-Aware Process Reward Modeling** | AffordRanker PRM on Qwen2.5-7B-Instruct: step-level safety and coherence scoring, GPT-4o labels, DeepSpeed ZeRO-2, reward-guided decoding with vLLM, jailbreak ASR | [README](01-risk-aware-process-reward-modeling/README.md) | [PDF](01-risk-aware-process-reward-modeling/01-risk-aware-process-reward-modeling.pdf) |
| 2 | **LLM Post-Training: SFT + DPO with LoRA** | 1.5B causal LM, SFT then DPO with TRL, LoRA/PEFT (8.4M params, 34 MB), W&B, GSM8K/MMLU and LLM-as-a-Judge win rate | [README](02-llm-post-training-sft-dpo/README.md) | [PDF](02-llm-post-training-sft-dpo/02-llm-post-training-sft-dpo.pdf) |
| 3 | **Fraud Detection & Risk Analysis** (ML edition) | Imbalanced classification at 0.17%, PR-AUC vs ROC-AUC, fold-local SMOTE, XGBoost + Optuna, SHAP vs UMAP, PSI drift, MLflow, pytest, CI | [README](03-fraud-detection-risk-analysis/README.md) | [PDF](03-fraud-detection-risk-analysis/03-fraud-detection-risk-analysis.pdf) |
| 4 | **Leakage-Safe Time-Series Forecasting & Backtesting** | Walk-forward with purging and embargo, AR(1)/ARIMA/XGBoost/GRU/BiLSTM, attention ensemble, MSE vs trading value, Sharpe, drawdown, turnover | [README](04-leakage-safe-time-series-forecasting/README.md) | [PDF](04-leakage-safe-time-series-forecasting/04-leakage-safe-time-series-forecasting.pdf) |

**Every playbook has the same seven parts:** (1) project overview, with the spoken answer to "Can you explain this project?" and a list of what to confirm; (2) fundamentals, one concept card per resume term (simple, technical, math, why it matters, in this project, interview angle); (3) methodology and workflow, including a numerical-claims table; (4) six design decisions with alternatives and trade-offs; (5) 2-, 5- and 10-minute spoken explanations; (6) 25–26 interview questions with teaching answers (mostly easy and medium); (7) a one-page revision sheet.

**Evidence status.** No code, logs or notebooks for these projects are in this folder, so every resume number is labelled UNVERIFIED (your reported result), worked-example numbers are ILLUSTRATIVE, and assumed implementation details are marked **[confirm]**. Each playbook's section 1.3 lists what to check before an interview.

The Fraud project also appears in [`../QUANT/05-fraud-detection-risk-analysis`](../QUANT/05-fraud-detection-risk-analysis/README.md), rewritten for risk and decision-threshold interviews.

**Rebuilding.** The README files are the source. Edit one, then from `../playbook-build`: `node playbooks.mjs DATA` (or `DATA/03`) and `.venv/bin/python qa.py`.
