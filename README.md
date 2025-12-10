# Urch Forum Econ PhD Admissions Analyzer

## 1. Project Goal

The goal of this project is to collect unstructured conversation data from the Urch Economics PhD Forums and transform it into a structured dataset to analyze admissions trends for Top 50 Economics PhD programs. While anecdotal evidence suggests that quantitative metrics (GRE, GPA) are primary filters for admissions, there is a lack of large-scale, public datasets confirming exactly where the "bar" lies for different tiers of schools. The primary goal of this project is to use generative AI to extract structured data from thousands of forum posts to identify correlations between applicant profiles and admission outcomes.

The key research questions are:

- **What are the "hard cutoffs" for GRE Quant scores and GPA across Top 10, Top 20, and Top 50 programs?**

- **How significant is the "Math Background" signal (e.g., Real Analysis grades) compared to aggregate GPA?**

- **Has the competitiveness of admissions (average GRE/GPA of successful applicants) increased over the last decade?**

---

##  2. Data Source and Collection
### Data Source
The data source for this project is the Urch Economics PhD Forum. This is a public discussion board where applicants historically post their "profiles" (stats, math background, research experience) and subsequent results (acceptances/rejections).

### Data Collection Method

The data pipeline follows a classic ETL (Extract, Transform, Load) process:

1.  **Thread Crawling:** A custom Python script crawls the forum index to identify thread URLs related to admission cycles (e.g., "Profile Evaluation Fall 2024").

2.  **Post Scraping:** The script iterates through identified threads, extracting raw HTML content, author metadata, and timestamps.

3.  **Cloud Storage:** Raw unstructured text is immediately loaded into a PostgreSQL database hosted on Google Cloud SQL to ensure data persistence and scalability.

4.  **Heuristic Filtering:** A pre-processing layer filters out 60% of noise (general chatter) by flagging posts containing high-value keywords (e.g., "GRE", "GPA", "Math", "Accept", "Reject").

5.  **LLM Extraction (Transformation):** We utilize OpenAI's GPT-5 Mini (Batch API) with structured function calling to parse the filtered text. The model extracts specific entities:

    * `undergrad_gpa`: Normalized to a 4.0 scale.
    * `gre_quant`: Extracted as a 130-170 score.
    * `math_maturity`: Ranked on an ordinal scale based on coursework mentioned (e.g., Real Analysis, Topology).
    * `admission_results`: Mapped to a standardized list of Top 50 universities.
