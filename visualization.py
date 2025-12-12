"""
PhD Economics Admissions Visualization:
Interactive Streamlit dashboard for analyzing PhD admissions data
"""

import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv
import os
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

load_dotenv()
db_params = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}


st.set_page_config(
    page_title="PhD Admissions Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .filter-section {
        background-color: #ffffff;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_db_connection():
    return psycopg2.connect(**db_params) #Database connection

def build_sql_query(filters):
    """
    Building SQL query with filters for each feature variable
    """
    
    base_query = """
    SELECT 
        COUNT(*) as total_applicants,
        SUM(CASE WHEN got_phd_offer = 1 THEN 1 ELSE 0 END) as accepted,
        SUM(CASE WHEN phd_accepted_rank IS NOT NULL THEN 1 ELSE 0 END) as has_placement,
        
        -- Acceptance rate by tier
        SUM(CASE WHEN got_phd_offer = 1 AND phd_accepted_rank = 1 THEN 1 ELSE 0 END) as accepted_tier1,
        SUM(CASE WHEN got_phd_offer = 1 AND phd_accepted_rank = 2 THEN 1 ELSE 0 END) as accepted_tier2,
        SUM(CASE WHEN got_phd_offer = 1 AND phd_accepted_rank = 3 THEN 1 ELSE 0 END) as accepted_tier3,
        SUM(CASE WHEN got_phd_offer = 1 AND phd_accepted_rank = 4 THEN 1 ELSE 0 END) as accepted_tier4,
        
        -- Average scores 
        AVG(CASE WHEN undergrad_gpa_std IS NOT NULL THEN undergrad_gpa_std END) as avg_gpa,
        AVG(CASE WHEN gre_quant_std IS NOT NULL THEN gre_quant_std END) as avg_gre_quant,
        AVG(CASE WHEN gre_verbal_std IS NOT NULL THEN gre_verbal_std END) as avg_gre_verbal,
        
        -- Course percentages 
        SUM(CASE WHEN taken_calculus = 1 THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN taken_calculus IS NOT NULL THEN 1 ELSE 0 END), 0) as pct_calculus,
        SUM(CASE WHEN taken_linear_algebra = 1 THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN taken_linear_algebra IS NOT NULL THEN 1 ELSE 0 END), 0) as pct_linear_algebra,
        SUM(CASE WHEN taken_real_analysis = 1 THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN taken_real_analysis IS NOT NULL THEN 1 ELSE 0 END), 0) as pct_real_analysis,
        
        -- Research experience 
        SUM(CASE WHEN research_experience = true THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN research_experience IS NOT NULL THEN 1 ELSE 0 END), 0) as pct_research,
        
        -- Econ major 
        SUM(CASE WHEN undergrad_econ_related = 1 THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN undergrad_econ_related IS NOT NULL THEN 1 ELSE 0 END), 0) as pct_econ_major
        
    FROM admissions_data_cleaned
    WHERE got_phd_offer IS NOT NULL  -- Only include those who actually applied
    """
    
    where_clauses = []


    if filters['gpa_range'][0] > 0 or filters['gpa_range'][1] < 4.0:
        where_clauses.append(
            f"undergrad_gpa_std BETWEEN {filters['gpa_range'][0]} AND {filters['gpa_range'][1]}"
        )
    

    if filters['gre_quant_range'][0] > 130 or filters['gre_quant_range'][1] < 170:
        where_clauses.append(
            f"gre_quant_std BETWEEN {filters['gre_quant_range'][0]} AND {filters['gre_quant_range'][1]}"
        )
    

    if filters['gre_verbal_range'][0] > 130 or filters['gre_verbal_range'][1] < 170:
        where_clauses.append(
            f"gre_verbal_std BETWEEN {filters['gre_verbal_range'][0]} AND {filters['gre_verbal_range'][1]}"
        )
    

    if filters['undergrad_rank']:
        rank_list = ','.join(map(str, filters['undergrad_rank']))
        where_clauses.append(f"undergrad_rank IN ({rank_list})")
    

    if filters['has_calculus']:
        where_clauses.append("taken_calculus = 1")
    if filters['has_linear_algebra']:
        where_clauses.append("taken_linear_algebra = 1")
    if filters['has_real_analysis']:
        where_clauses.append("taken_real_analysis = 1")
    

    if filters['has_research']:
        where_clauses.append("research_experience = true")
    

    if filters['econ_major']:
        where_clauses.append("undergrad_econ_related = 1")
    

    if filters['has_grad_program']:
        where_clauses.append("attended_grad_program = 1")
    

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)
    
    return base_query

def get_placement_by_tier(filters):
    """Get detailed placement statistics by university tier"""
    
    where_clauses = ["got_phd_offer = 1", "phd_accepted_rank IS NOT NULL"]
    

    if filters['gpa_range'][0] > 0 or filters['gpa_range'][1] < 4.0:
        where_clauses.append(
            f"undergrad_gpa_std BETWEEN {filters['gpa_range'][0]} AND {filters['gpa_range'][1]}"
        )
    if filters['gre_quant_range'][0] > 130 or filters['gre_quant_range'][1] < 170:
        where_clauses.append(
            f"gre_quant_std BETWEEN {filters['gre_quant_range'][0]} AND {filters['gre_quant_range'][1]}"
        )
    if filters['gre_verbal_range'][0] > 130 or filters['gre_verbal_range'][1] < 170:
        where_clauses.append(
            f"gre_verbal_std BETWEEN {filters['gre_verbal_range'][0]} AND {filters['gre_verbal_range'][1]}"
        )
    if filters['undergrad_rank']:
        rank_list = ','.join(map(str, filters['undergrad_rank']))
        where_clauses.append(f"undergrad_rank IN ({rank_list})")
    if filters['has_calculus']:
        where_clauses.append("taken_calculus = 1")
    if filters['has_linear_algebra']:
        where_clauses.append("taken_linear_algebra = 1")
    if filters['has_real_analysis']:
        where_clauses.append("taken_real_analysis = 1")
    if filters['has_research']:
        where_clauses.append("research_experience = true")
    if filters['econ_major']:
        where_clauses.append("undergrad_econ_related = 1")
    if filters['has_grad_program']:
        where_clauses.append("attended_grad_program = 1")
    
    query = f"""
    SELECT 
        phd_accepted_rank as tier,
        COUNT(*) as count,
        ROUND(AVG(CASE WHEN undergrad_gpa_std IS NOT NULL THEN undergrad_gpa_std END), 2) as avg_gpa,
        ROUND(AVG(CASE WHEN gre_quant_std IS NOT NULL THEN gre_quant_std END), 1) as avg_gre_q,
        ROUND(AVG(CASE WHEN gre_verbal_std IS NOT NULL THEN gre_verbal_std END), 1) as avg_gre_v
    FROM admissions_data_cleaned
    WHERE {' AND '.join(where_clauses)}
    GROUP BY phd_accepted_rank
    ORDER BY phd_accepted_rank
    """
    
    conn = get_db_connection()
    df = pd.read_sql(query, conn)
    
    # Map tiers to labels
    tier_labels = {1: 'Tier 1', 2: 'Tier 2', 3: 'Tier 3', 4: 'Tier 4'}
    df['tier_label'] = df['tier'].map(tier_labels)
    
    return df

def main():
    # Header
    st.markdown('<h1 class="main-header">PhD Economics Admissions Analytics</h1>', unsafe_allow_html=True)
    st.markdown("### Interactive dashboard for analyzing PhD admissions patterns and placement outcomes")
    
    # Sidebar filters
    st.sidebar.markdown("## Filters")
    st.sidebar.markdown("*Adjust filters to see how admission outcomes change*")
    
    filters = {}
    
    # Academic Performance Filters
    st.sidebar.markdown("### Academic Performance")
    filters['gpa_range'] = st.sidebar.slider(
        "Undergrad GPA (4.0 scale)",
        min_value=0.0,
        max_value=4.0,
        value=(0.0, 4.0),
        step=0.1
    )
    
    filters['gre_quant_range'] = st.sidebar.slider(
        "GRE Quantitative",
        min_value=130,
        max_value=170,
        value=(130, 170),
        step=1
    )
    
    filters['gre_verbal_range'] = st.sidebar.slider(
        "GRE Verbal",
        min_value=130,
        max_value=170,
        value=(130, 170),
        step=1
    )
    

    st.sidebar.markdown("### Background")
    
    undergrad_options = {
        1: "Tier 1",
        2: "Tier 2", 
        3: "Tier 3",
        4: "Tier 4",
        5: "Tier 5",
    }
    
    selected_ranks = st.sidebar.multiselect(
        "Undergrad Institution Tier",
        options=list(undergrad_options.keys()),
        format_func=lambda x: undergrad_options[x],
        default=[]
    )
    filters['undergrad_rank'] = selected_ranks if selected_ranks else None
    
    filters['econ_major'] = st.sidebar.checkbox("Economics-related major", value=False)
    filters['has_grad_program'] = st.sidebar.checkbox("Attended grad program", value=False)
    

    st.sidebar.markdown("### Math Preparation")
    filters['has_calculus'] = st.sidebar.checkbox("Took Calculus", value=False)
    filters['has_linear_algebra'] = st.sidebar.checkbox("Took Linear Algebra", value=False)
    filters['has_real_analysis'] = st.sidebar.checkbox("Took Real Analysis", value=False)
    

    st.sidebar.markdown("### 🔬 Experience")
    filters['has_research'] = st.sidebar.checkbox("Has research experience", value=False)
    

    if st.sidebar.button("🔄 Reset All Filters"):
        st.rerun()
    

    try:
        conn = get_db_connection()
        query = build_sql_query(filters)
        results = pd.read_sql(query, conn)
        
        if len(results) == 0 or results['total_applicants'].iloc[0] == 0:
            st.warning("⚠️ No applicants match the selected filters. Please adjust your criteria.")
            return
        

        total = results['total_applicants'].iloc[0]
        accepted = results['accepted'].iloc[0]
        acceptance_rate = (accepted / total * 100) if total > 0 else 0
        

        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Applicants",
                f"{total:,}",
                help="Number of applicants matching filters"
            )
        
        with col2:
            st.metric(
                "Acceptance Rate",
                f"{acceptance_rate:.1f}%",
                help="Percentage of applicants who received at least one PhD offer"
            )
        
        with col3:
            st.metric(
                "Accepted Students",
                f"{accepted:,}",
                help="Number of students who received PhD offers"
            )
        
        with col4:
            avg_gpa = results['avg_gpa'].iloc[0]
            st.metric(
                "Avg GPA",
                f"{avg_gpa:.2f}" if pd.notna(avg_gpa) else "N/A",
                help="Average undergraduate GPA (4.0 scale)"
            )
        

        st.markdown("---")
        st.markdown("##  PhD Placement Distribution")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:

            tier_data = get_placement_by_tier(filters)
            
            if len(tier_data) > 0:

                fig = px.pie(
                    tier_data,
                    values='count',
                    names='tier_label',
                    title='Distribution of PhD Placements by School Tier',
                    color_discrete_sequence=px.colors.sequential.Blues_r
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No placement data available for selected filters")
        
        with col2:
            st.markdown("### Placement Breakdown")
            
            tier1 = results['accepted_tier1'].iloc[0]
            tier2 = results['accepted_tier2'].iloc[0]
            tier3 = results['accepted_tier3'].iloc[0]
            tier4 = results['accepted_tier4'].iloc[0]
            total_placed = tier1 + tier2 + tier3 + tier4
            
            if total_placed > 0:
                st.markdown(f"**Tier 1:** {tier1} ({tier1/total_placed*100:.1f}%)")
                st.markdown(f"**Tier 2:** {tier2} ({tier2/total_placed*100:.1f}%)")
                st.markdown(f"**Tier 3:** {tier3} ({tier3/total_placed*100:.1f}%)")
                st.markdown(f"**Tier 4:** {tier4} ({tier4/total_placed*100:.1f}%)")
            else:
                st.info("No placement data")
        

        st.markdown("---")
        st.markdown("##  Detailed Statistics")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("### Test Scores")
            avg_gre_q = results['avg_gre_quant'].iloc[0]
            avg_gre_v = results['avg_gre_verbal'].iloc[0]
            
            if pd.notna(avg_gre_q):
                st.metric("Avg GRE Quant", f"{avg_gre_q:.1f}")
            else:
                st.metric("Avg GRE Quant", "N/A")
                
            if pd.notna(avg_gre_v):
                st.metric("Avg GRE Verbal", f"{avg_gre_v:.1f}")
            else:
                st.metric("Avg GRE Verbal", "N/A")
        
        with col2:
            st.markdown("### Math Courses")
            pct_calc = results['pct_calculus'].iloc[0]
            pct_la = results['pct_linear_algebra'].iloc[0]
            pct_ra = results['pct_real_analysis'].iloc[0]
            
            st.metric("Calculus", f"{pct_calc:.1f}%" if pd.notna(pct_calc) else "N/A")
            st.metric("Linear Algebra", f"{pct_la:.1f}%" if pd.notna(pct_la) else "N/A")
            st.metric("Real Analysis", f"{pct_ra:.1f}%" if pd.notna(pct_ra) else "N/A")
        
        with col3:
            st.markdown("### Background")
            pct_research = results['pct_research'].iloc[0]
            pct_econ = results['pct_econ_major'].iloc[0]
            
            st.metric("Research Experience", f"{pct_research:.1f}%" if pd.notna(pct_research) else "N/A")
            st.metric("Econ Major", f"{pct_econ:.1f}%" if pd.notna(pct_econ) else "N/A")
        

        if len(tier_data) > 0:
            st.markdown("---")
            st.markdown("## Placement Details by Tier")
            
            display_df = tier_data.copy()
            display_df['percentage'] = (display_df['count'] / display_df['count'].sum() * 100).round(1)
            display_df = display_df[['tier_label', 'count', 'percentage', 'avg_gpa', 'avg_gre_q', 'avg_gre_v']]
            display_df.columns = ['Tier', 'Students', '%', 'Avg GPA', 'Avg GRE-Q', 'Avg GRE-V']
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
        

        st.markdown("---")
        st.markdown("*Data excludes blank/null entries. Percentages calculated only from applicants with relevant data.*")
        
    except Exception as e:
        st.error(f"Error executing query: {e}")
        st.exception(e)

if __name__ == "__main__":
    main()