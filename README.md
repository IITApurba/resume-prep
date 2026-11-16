# Resume Prep

Interview preparation material for every project on my resume, organised by role track. Each project has a print-ready PDF playbook (about 20–25 A4 pages) that explains the project from first principles and prepares me to defend each resume bullet in a 30–45 minute interview.

| Track | Projects | Built for |
|---|---|---|
| [**SDE**](#sde--software-engineering) | 4 | Backend and distributed-systems interviews |
| [**DATA**](#data--data-science--ml-engineering) | 4 | Data science, ML engineering and AI engineering interviews |
| [**QUANT**](#quant--quant-research--development) | 5 | Quant research and quant development interviews |

All PDFs are generated from Markdown in this repo, so the Markdown is the source of truth and the PDFs can be rebuilt at any time (see [Building the PDFs](#building-the-pdfs)).

---

## SDE: Software Engineering

Each SDE project has three documents: a **design document** (the reference architecture), a **25-page playbook** for everyday revision, and a **140–160-page study guide** for deep reference.

| # | Project | Stack | Design doc | Playbook | Study guide |
|---|---|---|---|---|---|
| 1 | **High-Concurrency Payment & Transaction Processing** | Java 21, Spring Boot, PostgreSQL, Kafka, Redis | [MD](SDE/01-payment-transaction-processing-system/PAYMENT_TRANSACTION_PROCESSING_SYSTEM.md) | [PDF](SDE/interview-preparation/final-playbooks/01-payment-transaction-processing.pdf) | [PDF](SDE/interview-preparation/pdfs/01-payment-transaction-processing-study-guide.pdf) |
| 2 | **Distributed Object Storage System** | C++20, gRPC, RocksDB, consistent hashing, quorums, WAL | [MD](SDE/02-distributed-object-storage-system/DISTRIBUTED_OBJECT_STORAGE_SYSTEM.md) | [PDF](SDE/interview-preparation/final-playbooks/02-distributed-object-storage.pdf) | [PDF](SDE/interview-preparation/pdfs/02-distributed-object-storage-study-guide.pdf) |
| 3 | **Real-Time Event Processing Platform** | Kafka, Apache Flink, Redis, PostgreSQL, AWS | [MD](SDE/03-real-time-event-processing-platform/REAL_TIME_EVENT_PROCESSING_PLATFORM.md) | [PDF](SDE/interview-preparation/final-playbooks/03-real-time-event-processing.pdf) | [PDF](SDE/interview-preparation/pdfs/03-real-time-event-processing-study-guide.pdf) |
| 4 | **Production LLM Gateway & Inference Router** | Spring Boot, Redis, Kafka, Prometheus, Kubernetes (EKS) | [MD](SDE/04-llm-gateway-inference-router/LLM_GATEWAY_INFERENCE_ROUTER.md) | [PDF](SDE/interview-preparation/final-playbooks/04-llm-gateway-inference-router.pdf) | [PDF](SDE/interview-preparation/pdfs/04-llm-gateway-inference-router-study-guide.pdf) |

Every SDE playbook has six parts: fundamentals (one concept card per resume term), high-level design, how the project works end to end, 2/5/10-minute spoken explanations, 28 interview questions with teaching answers, and a one-page revision sheet. More detail in [`SDE/interview-preparation/final-playbooks/README.md`](SDE/interview-preparation/final-playbooks/README.md).

## DATA: Data Science & ML Engineering

| # | Project | Highlights | PDF |
|---|---|---|---|
| 1 | [**Risk-Aware Process Reward Modeling**](DATA/01-risk-aware-process-reward-modeling/README.md) | Step-level PRM on Qwen2.5-7B, DeepSpeed ZeRO-2, reward-guided decoding with vLLM | [PDF](DATA/01-risk-aware-process-reward-modeling/01-risk-aware-process-reward-modeling.pdf) |
| 2 | [**LLM Post-Training: SFT + DPO with LoRA**](DATA/02-llm-post-training-sft-dpo/README.md) | TRL, LoRA/PEFT, GSM8K/MMLU, LLM-as-a-judge win rate | [PDF](DATA/02-llm-post-training-sft-dpo/02-llm-post-training-sft-dpo.pdf) |
| 3 | [**Fraud Detection & Risk Analysis**](DATA/03-fraud-detection-risk-analysis/README.md) (ML edition) | Extreme class imbalance, PR-AUC, XGBoost + Optuna, SHAP, PSI drift, MLflow | [PDF](DATA/03-fraud-detection-risk-analysis/03-fraud-detection-risk-analysis.pdf) |
| 4 | [**Leakage-Safe Time-Series Forecasting**](DATA/04-leakage-safe-time-series-forecasting/README.md) | Purged walk-forward, ARIMA/XGBoost/GRU/BiLSTM, attention ensemble | [PDF](DATA/04-leakage-safe-time-series-forecasting/04-leakage-safe-time-series-forecasting.pdf) |

Track overview: [`DATA/INDEX.md`](DATA/INDEX.md)

## QUANT: Quant Research & Development

| # | Project | Highlights | PDF |
|---|---|---|---|
| 1 | [**Limit Order Book Alpha Research**](QUANT/01-limit-order-book-alpha-research/README.md) | Order-flow imbalance, microprice, LR/XGBoost/GRU, cost-aware thresholds | [PDF](QUANT/01-limit-order-book-alpha-research/01-limit-order-book-alpha-research.pdf) |
| 2 | [**Cross-Sectional Alpha Discovery**](QUANT/02-cross-sectional-alpha-discovery/README.md) | Point-in-time factors, IC, neutralisation, multiple testing, vol-targeted portfolios | [PDF](QUANT/02-cross-sectional-alpha-discovery/02-cross-sectional-alpha-discovery.pdf) |
| 3 | [**Market Impact & Optimal Execution**](QUANT/03-market-impact-optimal-execution/README.md) | Square-root law, Almgren-Chriss, TWAP/VWAP/POV, queue-aware simulation | [PDF](QUANT/03-market-impact-optimal-execution/03-market-impact-optimal-execution.pdf) |
| 4 | [**Real-Time Portfolio Risk & Stress Testing**](QUANT/04-portfolio-risk-stress-testing/README.md) | VaR / ES, Ledoit-Wolf shrinkage, factor exposures, stress scenarios | [PDF](QUANT/04-portfolio-risk-stress-testing/04-portfolio-risk-stress-testing.pdf) |
| 5 | [**Fraud Detection & Risk Analysis**](QUANT/05-fraud-detection-risk-analysis/README.md) (risk edition) | Cost-optimal thresholds, calibration, capacity-constrained decision tiers | [PDF](QUANT/05-fraud-detection-risk-analysis/05-fraud-detection-risk-analysis.pdf) |

Track overview: [`QUANT/INDEX.md`](QUANT/INDEX.md)

Every DATA and QUANT playbook has seven parts: project overview, fundamentals (concept cards with the maths written out and every variable defined), methodology, six design decisions with trade-offs, 2/5/10-minute spoken explanations, 25–26 interview questions with teaching answers, and a one-page revision sheet.

---

## A note on numbers

These are preparation notes, not project repositories. No source code, logs or benchmarks are stored here. The playbooks therefore label resume figures as **UNVERIFIED** (reported results), worked examples as **ILLUSTRATIVE**, and assumed implementation details as **[confirm]**. All Sharpe, drawdown and cost figures in the QUANT track come from simulated backtests, never live trading.

---

## Repository layout

```
.
├── SDE/
│   ├── 0N-<project>/                 # design document per project
│   └── interview-preparation/
│       ├── 0N-*-study-guide.md       # long study guides (source)
│       ├── pdfs/                     # study-guide PDFs (140–160 pages)
│       ├── final-playbooks/          # 25-page playbooks + src/*.md
│       └── build/                    # SDE PDF toolchain
├── DATA/
│   ├── INDEX.md
│   └── 0N-<project>/                 # README.md (source) + PDF
├── QUANT/
│   ├── INDEX.md
│   └── 0N-<project>/                 # README.md (source) + PDF
└── playbook-build/                   # DATA + QUANT PDF toolchain
```

---

## Building the PDFs

There are two toolchains: one for the SDE material and one for DATA + QUANT. Both turn Markdown into PDFs the same way: markdown-it, then Mermaid diagrams rendered as vector SVGs, then headless Chrome. The DATA + QUANT build also renders KaTeX equations. Each build prints the PDF twice so the table of contents carries real page numbers, and each comes with a QA script that checks page counts, fonts, links and overflow.

**Requirements:** Node.js 18+, Python 3, and Google Chrome (set `CHROME_PATH` if it is not at `/Applications/Google Chrome.app`).

**One-time setup** (the dependency folders are not committed):

```sh
cd playbook-build                       # or SDE/interview-preparation/build
npm install
python3 -m venv .venv && .venv/bin/pip install pymupdf
```

**DATA + QUANT:**

```sh
cd playbook-build
node playbooks.mjs              # all nine playbooks
node playbooks.mjs QUANT        # one track
node playbooks.mjs DATA/03      # one playbook
.venv/bin/python qa.py          # QA report + grayscale contact sheets
```

**SDE:**

```sh
cd SDE/interview-preparation/build
node playbooks.mjs              # the four 25-page playbooks (or: node playbooks.mjs 03)
.venv/bin/python qa_playbooks.py
node build.mjs                  # the long study guides
```

The full Markdown conventions (concept cards, question badges, callout boxes, figure scaling) are documented in [`playbook-build/README.md`](playbook-build/README.md) and [`SDE/interview-preparation/build/README.md`](SDE/interview-preparation/build/README.md).
