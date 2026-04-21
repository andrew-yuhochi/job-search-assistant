# Knowledge Bank — Andrew Yu

This document captures my professional experience, technical achievements, and project outcomes for use in generating tailored CV highlight sections.

---

## Toronto Union (TU) — Data Science, 2022–2025

I worked as a Senior Data Scientist at Toronto Union, a Series B fintech that processes payroll and benefits for mid-market Canadian employers. The team of eight data scientists sat within the AI Platform group.

### Payroll Anomaly Detection System

- Built a production payroll anomaly detection model using an Isolation Forest ensemble combined with a rule-based layer, reducing manual payroll review time by 62% across 1,200+ employer accounts.
- Designed the feature engineering pipeline in PySpark on Databricks: 45 features derived from rolling 13-week windows, including variance in pay run totals, new employee onboarding spikes, and retroactive adjustment frequency.
- Achieved 94% precision and 88% recall on a labelled evaluation set of 6,200 payroll anomaly events, compared to 71% precision for the previous rules-only system.
- Deployed the model as a FastAPI microservice on AWS ECS with SageMaker batch inference for nightly scoring; latency p95 was 120ms for synchronous requests.
- Reduced false-positive payroll holds by 38%, directly improving customer satisfaction scores (CSAT) for the payroll operations team by 14 points.

### Churn and Revenue Retention Models

- Owned the customer churn prediction model (XGBoost) that powered the customer success team's outreach prioritization queue; model predicted 30-day churn with AUC of 0.89 on a held-out test set.
- Engineered 120+ features from product usage telemetry, support ticket history, and payroll processing patterns using dbt models over Snowflake; managed the dbt project and model documentation.
- A/B tested the model-driven outreach cadence against the previous manual prioritization; model-driven outreach improved 90-day net revenue retention by 7.2 percentage points (statistically significant at p < 0.01, n = 3,400 accounts).
- Built the feature store layer for churn-related features using Feast on AWS, enabling same-day feature freshness for the online inference path.
- Presented quarterly churn model performance reviews to the VP of Customer Success and CFO; translated model metrics (AUC, KS statistic, lift curves) into business language (expected revenue saved, outreach ROI).

### Experimentation and Data Platform Contributions

- Designed and implemented TU's internal A/B testing framework using Python, supporting up to 12 concurrent experiments; handled imbalanced traffic splits and novelty effects via sequential testing (SPRT methodology).
- Led the migration of the ML feature pipeline from a legacy Airflow 1.x DAG spaghetti structure to a modular Airflow 2.x TaskFlow API design, reducing pipeline maintenance incidents by 55%.
- Mentored two junior data scientists on model evaluation best practices, causal inference for experiments, and stakeholder communication; both were promoted within 18 months.

---

## Recursive Analytics — ML Engineering, 2019–2022

Recursive Analytics was a boutique ML consultancy based in Vancouver working with mid-market clients in retail, real estate, and financial services. I joined as a Data Scientist and transitioned into an ML Engineering focus over three years.

### Retail Demand Forecasting (Client: National Grocery Chain)

- Built a hierarchical demand forecasting system using LightGBM with Optuna hyperparameter optimization for a national grocery client with 450 SKUs and 80 store locations.
- Reduced forecast MAPE from 18.3% (previous ARIMA-based system) to 9.7% at the SKU-store level, translating to a $2.1M reduction in estimated annual shrinkage for the client.
- Designed the inference architecture: models were serialized with MLflow, versioned in S3, and served via a Flask API behind AWS API Gateway; the client's ERP polled the API nightly.
- Automated retraining pipelines using Airflow DAGs triggered by data drift detection (PSI > 0.25 threshold on key categorical features).

### Real-Time Recommendation Engine (Client: Proptech SaaS)

- Architected and deployed a real-time property recommendation engine for a Vancouver proptech SaaS serving 400K monthly active users.
- Implemented a two-tower neural retrieval model (user embedding tower + property embedding tower) using PyTorch and served embeddings via Faiss flat index; retrieval latency p99 was 35ms.
- Built the online feature serving layer using Redis for real-time user context (recently viewed properties, saved search filters) with a 24-hour TTL.
- Increased click-through rate (CTR) on recommended properties from 3.2% to 7.8% (143% relative uplift) in a holdout A/B test over 8 weeks, n = 180,000 users.
- Managed the full deployment lifecycle from prototype to production: containerized with Docker, deployed on GKE, monitored with Prometheus and Grafana dashboards for drift and latency.

### ML Infrastructure and Internal Tooling

- Standardized the ML project template across all client engagements: cookie-cutter project structure, shared CI/CD pipeline using GitHub Actions, and a lightweight model card template for documentation.
- Reduced average time from model prototype to client demo from 6 weeks to 3.5 weeks by centralizing reusable feature engineering utilities and model evaluation harnesses in an internal Python package.
- Contributed to the company's pre-sales process by running technical discovery workshops, writing scoping proposals, and delivering proof-of-concept Jupyter notebooks that demonstrated model feasibility before contract signature.

---

## University of British Columbia — Graduate Research, 2017–2019

I completed a Master of Data Science at UBC, supervised by Professor [Redacted] in the Statistical Machine Learning lab. My research focused on uncertainty quantification in neural networks for tabular clinical data.

### Research: Uncertainty-Aware Classification for Clinical Decision Support

- Developed a Monte Carlo Dropout ensemble approach for uncertainty estimation in a deep neural network trained on the MIMIC-III ICU clinical dataset (48,000 patient stays).
- Demonstrated that uncertainty-aware models flagged 31% of predictions as high-uncertainty; routing high-uncertainty cases to human review reduced overall error rate by 22% compared to a fixed decision threshold.
- Published findings at the Canadian AI conference (CAnAI 2019) and presented a poster at NeurIPS 2019 workshop on Bayesian Deep Learning.
- Implemented all experiments in PyTorch; made the codebase publicly available on GitHub (250+ stars).

### Teaching Assistantship and Coursework

- Served as a TA for DSCI 571 (Supervised Learning) for two semesters, holding weekly office hours for 80+ students and grading assignments on classification, model selection, and cross-validation.
- Completed graduate coursework in probabilistic graphical models, natural language processing, and convex optimization.
