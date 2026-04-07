# 📊 Job Market Insights Pipeline

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)

An end-to-end data engineering pipeline that scrapes, processes, and enriches job market data to provide actionable insights through an interactive dashboard.

🔗 **Live Demo:** [Data Jobs Pulse Dashboard](https://jobmarket-data-pipeline-bart.streamlit.app/)

<img width="1190" height="969" alt="image" src="https://github.com/user-attachments/assets/b088be78-39af-46ae-bb59-c16382c7ddd5" />


---

## 🎯 Business Problem & Objective

The job market for data professionals is highly fragmented, with constantly shifting skill requirements and inconsistent naming conventions across companies. Relying on generic, static industry reports makes it difficult to understand real-time market demands and accurately evaluate compensation standards. The primary goal of this project was to tackle this information gap by building an automated, self-updating data system that extracts, standardizes, and analyzes live job postings to provide actionable, data-driven insights into the tech landscape.

---

## 🚀 Project Overview

This project demonstrates a complete modern data stack workflow. It automates the journey of raw, unstructured job advertisements from the web into a polished, LLM-enriched analytical gold mine.

### Key Features:
* **Automated Extraction:** Robust web scraping of public job portals.
* **LLM Enrichment:** Using Large Language Models to unify inconsistent job titles and extract specific tech stacks from descriptions.
* **Modern Analytics:** High-performance OLAP processing using DuckDB and modular modeling with dbt.
* **Interactive UI:** A sleek Streamlit dashboard for real-time data exploration.

### 💡 Key Insights Generated:
* **Skill Demand Quantification:** Directly maps which programming languages and tools (Python, dbt, SQL...) are currently driving the most demand across various data roles.
* **Salary Transparency:** Uncovers compensation bounds across different seniority levels and work models (remote vs. hybrid/office), providing clear benchmarks for salary evaluation.
* **Title Normalization:** Successfully maps wildly different job position names into standardized industry roles, allowing for accurate market trend analysis.

---

## 🛠 Tech Stack

| Layer | Technology |
| :--- | :--- |
| **Ingestion** | Python |
| **Storage & Compute** | DuckDB |
| **Transformation** | dbt (data build tool) |
| **AI/Enrichment** | LLM Integration (OpenAI API) |
| **Visualization** | Streamlit |

---

## 🏗 Data Pipeline Architecture

1.  **Web Scraping:** Systematic collection of raw job postings.
2.  **Data Cleaning:** Handling missing values, deduplication, and schema enforcement in Python.
3.  **LLM Enrichment:** * Unifying diverse job titles into standard categories.
    * Extracting hidden requirements (e.g., specific libraries or soft skills).
4.  **Data Modeling (dbt):** * Building a clean star schema.
    * Running data quality tests.
5.  **Analytics Layer:** Final presentation via Streamlit, hosted on the Cloud.

---

## ⚖️ Ethics & Data Privacy

Data integrity and ethical scraping were the highest priorities during the development of this project:

* **Public Data Only:** Only publicly accessible job listings were collected.
* **Non-Commercial Use:** This project was created strictly for analytical and educational purposes, not for commercial gain.
* **Respectful Scraping:** Implemented rate-limiting and polite headers to ensure zero impact on the performance of source servers (anti-spam compliance).
* **Privacy:** No personal data or identifiable user information was processed.

---

## 📈 Dashboard Preview

The final dashboard allows users to filter by:
* Unified offer title.
* Salary ranges across different seniority levels.
* Workplace type.
* Company size.

