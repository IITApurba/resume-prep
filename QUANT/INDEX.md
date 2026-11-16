# QUANT: Project Interview Playbooks

Five concise playbooks (19–20 A4 pages each) for Quant Research and Quant Development interviews of 30–45 minutes. Each explains the market mechanics, mathematics and statistics behind a resume project, with every equation's variables defined, and prepares you to defend every bullet.

| # | Project | What it covers | Source | PDF |
|---|---|---|---|---|
| 1 | **Limit Order Book Alpha Research** | LOB mechanics, imbalance, microprice, OFI, 120-tick labels, LR/XGBoost/GRU, purged walk-forward, AUC vs IC, cost-aware thresholds, OOS Sharpe | [README](01-limit-order-book-alpha-research/README.md) | [PDF](01-limit-order-book-alpha-research/01-limit-order-book-alpha-research.pdf) |
| 2 | **Cross-Sectional Alpha Discovery & Portfolio Construction** | Point-in-time data, momentum/reversal/volatility/liquidity factors, IC, purged K-fold, neutralisation, multiple testing, Sharpe decay, constrained vol-targeted portfolios | [README](02-cross-sectional-alpha-discovery/README.md) | [PDF](02-cross-sectional-alpha-discovery/02-cross-sectional-alpha-discovery.pdf) |
| 3 | **Market Impact & Optimal Execution** | Implementation shortfall, temporary vs permanent impact, square-root law, Almgren-Chriss, TWAP/VWAP/POV, queue-aware simulation, adaptive participation | [README](03-market-impact-optimal-execution/README.md) | [PDF](03-market-impact-optimal-execution/03-market-impact-optimal-execution.pdf) |
| 4 | **Real-Time Portfolio Risk & Stress Testing** | Portfolio variance, VaR (historical, parametric, Monte Carlo), ES, beta, marginal risk, factor exposures, Ledoit-Wolf shrinkage, stress tests, FastAPI/PostgreSQL service | [README](04-portfolio-risk-stress-testing/README.md) | [PDF](04-portfolio-risk-stress-testing/04-portfolio-risk-stress-testing.pdf) |
| 5 | **Fraud Detection & Risk Analysis** (risk edition) | Fraud as financial risk, base rates, cost-optimal thresholds, calibration, decision tiers under capacity, SHAP/UMAP, PSI monitoring, model validation | [README](05-fraud-detection-risk-analysis/README.md) | [PDF](05-fraud-detection-risk-analysis/05-fraud-detection-risk-analysis.pdf) |

**Every playbook has the same seven parts:** (1) project overview, with the spoken answer to "Can you explain this project?" and a list of what to confirm; (2) fundamentals, one concept card per resume term (simple, technical, math, why it matters, in this project, interview angle); (3) methodology and workflow, including a numerical-claims table; (4) six design decisions with alternatives and trade-offs; (5) 2-, 5- and 10-minute spoken explanations; (6) 26 interview questions with teaching answers (16 easy, 9 medium, 1 hard); (7) a one-page revision sheet.

**Evidence status.** No code, data or backtest logs for these projects are in this folder, so every resume number is labelled UNVERIFIED (your reported result), worked-example numbers are ILLUSTRATIVE, and assumed implementation details are marked **[confirm]**. All Sharpe, drawdown and cost figures are simulated backtest results, never live performance; the playbooks keep gross vs net, in-sample vs out-of-sample and forecast vs strategy quality separate.

The Fraud project also appears in [`../DATA/03-fraud-detection-risk-analysis`](../DATA/03-fraud-detection-risk-analysis/README.md), written for ML-engineering interviews.

**Rebuilding.** The README files are the source. Edit one, then from `../playbook-build`: `node playbooks.mjs QUANT` (or `QUANT/02`) and `.venv/bin/python qa.py`.
