# Analysis of Economics PhD Admissions via Urch Forum (2010-2025)

## 1. Project Goal

The goal of this project is to collect text data from the Urch Economics PhD Forum to create a structured dataset and analyze admissions trends for Economics PhD admissions over 2010-2025. It is widely understood that certain quantitative metrics (such as GRE, GPA, years of experience, etc.) are primary determinants of admission, but there is still a lot of ambiguity on the exact measures PhD aspirants must take to be placed into a program. The primary goal of this project is to leverage Generative AI tools, SQL databases, and Machine Learning to extract structured applicant profiles from thousands of forum posts and identify correlations between applicant background and admission outcomes.

The key research questions are:

- **What are "strong cutoffs" for GRE Quant scores and GPA across different tiers of PhD programs?**

- **How important is having a "Math Background" (e.g., Real Analysis, Linear Algebra, and Calculus classes) for successful admission?**

- **Can Machine Learning models accurately predict the probability of receiving a PhD offer and the relevant ranking of the offer based on self-reported stats?**

---

##  2. Data Source and Collection
### Data Source
The data for this project was directly scraped from the **Urch Economics PhD Forum**. This is a public discussion board where applicants historically post their "profiles" (stats, math background, research experience) and subsequent results (acceptances/rejections).

### Data Collection Method

The data collection and processing followed a rigorous 7-stage workflow:

1.  **Web Scraping:** A custom Python script crawled the Urch Forum (ID 104) to extract thread titles, timestamps, and post content, collecting **130,000+ raw posts**.

2.  **Cloud Uploading:** The raw text data was uploaded into a **PostgreSQL database hosted on Google Cloud SQL** for scalable storage.

3.  **Dataset Filtering:** To minimize downstream API costs, we applied a rule-based filter (post length, keyword density, title info, content quality, etc.) to identify "Profile Evaluation" and "Results" posts. This reduced the dataset from **130,000 raw observations** to **18,500 high-signal posts** (85% noise reduction).

4.  **GPT Tools Call:** We utilized **OpenAI's GPT-4o-mini** with the Tools/Function Calling API to extract structured information (GPA, GRE Quant, School Lists) from the filtered text. This process ran via 10 concurrent async workflows over ~6 hours.

5.  **Feature Engineering:** We standardized the raw AI outputs into analytical features:
    * `undergrad_gpa_std`: Standardized diverse scales (10.0, 100%, 4.3) to a 4.0 scale.
    * 'gre_quant_std' and 'gre_verbal_std': standardized GRE scores to account for old and new versions of the test.
    * `undergrad_rank`: Mapped institutions to **QS Economics Rankings 2025** (Tiers 1-5).
    * `taken_real_analysis`/'taken_linear_algebra'/'taken_calculus': Created indicator variables for advanced math coursework.

### Data Dictionary
The final dataset (`admissions_data_cleaned.csv`) is organized with the following columns:
| Column                   | Description                                                                                                                      |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `undergrad_gpa_std`      | The applicant's GPA normalized to a 4.0 scale.                                                                                   |
| `attended_grad_program`  | A boolean flag (`1`/`0`) indicating whether the applicant attended a prior graduate program (e.g., Master's).                    |
| `taken_calculus`         | A boolean flag (`1`/`0`) indicating if the applicant took Calculus.                                                              |
| `taken_linear_algebra`   | A boolean flag (`1`/`0`) indicating if the applicant took Linear Algebra.                                                        |
| `taken_real_analysis`    | A boolean flag (`1`/`0`) indicating if the applicant took Real Analysis.                                                         |
| `gre_quant_std`          | GRE Quantitative section score (standardized to the current 130–170 scale).                                                      |
| `gre_verbal_std`         | GRE Verbal section score (standardized to the current 130–170 scale).                                                            |
| `undergrad_econ_related` | A boolean flag (`1`/`0`) indicating whether the undergraduate major is Economics or a closely related field.                     |
| `academic_lor`           | A boolean flag (`1`/`0`) indicating whether the applicant has at least one academic letter of recommendation.                    |
| `professional_lor`       | A boolean flag (`1`/`0`) indicating whether the applicant has at least one professional (non-academic) letter of recommendation. |
| `undergrad_rank`         | The tier of the applicant's undergraduate institution (1 = Top 10, 5 = Unranked).                                                |
| `got_phd_offer`          | A boolean target variable (`1`/`0`) indicating if the applicant received at least one PhD offer.                                 |
| `phd_accepted_rank`      | The tier of the highest-ranked PhD program the applicant was admitted to.                                                        |

### Limitations of the Data

* **Self-Selection Bias:** Successful applicants with high stats are more likely to post their results than those who were rejected everywhere, potentially skewing the data upwards.
* **Sparsity:** Despite filtering, many posts contain incomplete profiles (e.g., listing GPA but not GRE), resulting in missing values for certain features.
* **Verification:** All data is self-reported and cannot be independently verified against official university records.

---

## 3. Analysis and Findings
### Methodology
The analysis is conducted using Python (`pandas`, `scikit-learn`) and visualized using `Streamlit`. We filtered the dataset to exclude incomplete profiles and categorized universities into tiers to visualize the "step function" of admissions requirements. We then trained Random Forest and Logistic Regression models to predict admission probabilities.

### Visualizations

#### Part 1: The "Hard" Filters
The following histograms illustrate the distribution of GRE Quant scores for successful applicants. The lack of left-tail density suggests a "hard cutoff" exists for top-tier programs.

**Figure 1: GRE Quant Score Distribution by School Tier**


#### Part 2: The GPA vs. Tier Correlation
We analyze the relationship between GPA and school rank. While Top 10 schools show a tight clustering near 4.0, lower-tier schools show significantly higher variance.

**Figure 2: Boxplot of GPA by Admission Tier**


#### Part 3: Math Background Importance
To test the hypothesis that math background is critical, we analyzed acceptance rates based on the completion of Real Analysis.

**Figure 3: Acceptance Rate by Math Background**


### Limitations of the Analysis

* **Correlation vs. Causation:** We can observe that high GRE scores correlate with admission, but cannot control for unobserved variables like Letters of Recommendation (LORs), which are often the decisive factor.
* **Sample Size:** The number of "complete" profiles for the most recent year (2025) is small compared to historical years, as the cycle is ongoing.

---

## 4. Extensions and Future Research

* **Sentiment Analysis:** Future work could analyze the sentiment of "Statement of Purpose" advice threads to correlate writing style with success.
* **Letter of Recommendation Proxy:** We could refine the LLM extraction to search for mentions of "famous recommenders" to create a proxy variable for LOR quality.
* **Predictive Dashboard:** Deploy the trained model as a public web app where future applicants can input their stats to get a predicted probability range.

### Appendix

1.  Clone the repository.
2.  Install the required dependencies using ().
3.  Run the code () to collect raw forum data.
4.  Run the code () to upload raw data to Cloud SQL.
5.  Run the code () to filter noise (130k -> 18.5k posts).
6.  Run the code () to extract structured profiles via OpenAI API.
7.  Run the code () to standardize and rank the data.
8.  Run () to view the interactive visualizations.
