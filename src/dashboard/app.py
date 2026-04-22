"""
Streamlit Dashboard — Job Market Data Platform
Consome exclusivamente a FastAPI (arquitetura de produção real)
Dark mode premium consistente com o Projeto 2
"""

import requests
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

API_BASE = "http://fastapi_app:8000"

# ── Page config ───────────────────────────────────────────────
st.set_page_config(
    page_title="Job Market Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Dark mode CSS ─────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0e1117; color: #fafafa; }
    .metric-card {
        background: #1e2130;
        border: 1px solid #2d3250;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .metric-value { font-size: 2rem; font-weight: 700; color: #4f8ef7; }
    .metric-label { font-size: 0.85rem; color: #8892a4; margin-top: 4px; }
    .section-title {
        font-size: 1.1rem; font-weight: 600;
        color: #c9d1d9; margin: 1.5rem 0 0.5rem;
        border-left: 3px solid #4f8ef7;
        padding-left: 10px;
    }
</style>
""", unsafe_allow_html=True)

PLOTLY_THEME = dict(
    paper_bgcolor="#0e1117",
    plot_bgcolor="#0e1117",
    font_color="#c9d1d9",
    margin=dict(l=20, r=20, t=30, b=20),
)


# ── API helpers ───────────────────────────────────────────────
@st.cache_data(ttl=300)
def fetch(endpoint: str, params: dict = None):
    try:
        r = requests.get(f"{API_BASE}{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return []


# ── Sidebar filters ───────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/48/combo-chart.png", width=40)
    st.title("Job Market\nPlatform")
    st.divider()

    country_opts = ["All", "US", "GB", "BR", "AT", "REMOTE"]
    country = st.selectbox("Country", country_opts)
    seniority_opts = ["All", "junior", "mid", "senior"]
    seniority = st.selectbox("Seniority", seniority_opts)
    salary_min = st.slider("Min Salary (USD)", 0, 200000, 0, step=5000)
    days = st.slider("Trend window (days)", 7, 90, 30)
    st.divider()
    st.caption("Data refreshes every 5 minutes")

country_param   = None if country == "All" else country
seniority_param = None if seniority == "All" else seniority

# ── Header ────────────────────────────────────────────────────
st.markdown("## 📊 Global Job Market — Data Engineering")
st.markdown("Real-time analytics powered by Airflow + PySpark + FastAPI")
st.divider()

# ── KPIs ─────────────────────────────────────────────────────
jobs_data = fetch("/jobs", {
    "limit": 500,
    **({"country": country_param} if country_param else {}),
    **({"seniority": seniority_param} if seniority_param else {}),
    **({"salary_min": salary_min} if salary_min > 0 else {}),
})
jobs_df = pd.DataFrame(jobs_data)

salaries_data = fetch("/salaries", {**({"country": country_param} if country_param else {})})
sal_df = pd.DataFrame(salaries_data)

companies_data = fetch("/companies", {
    "top_n": 10,
    **({"country": country_param} if country_param else {}),
})
comp_df = pd.DataFrame(companies_data)

trends_data = fetch("/trends", {
    "days": days,
    **({"country": country_param} if country_param else {}),
})
trend_df = pd.DataFrame(trends_data)

col1, col2, col3, col4 = st.columns(4)

total_jobs   = len(jobs_df)
avg_salary   = sal_df["avg_min"].mean() if not sal_df.empty else 0
top_country  = jobs_df["country"].value_counts().idxmax() if not jobs_df.empty else "—"
remote_pct   = (jobs_df["is_remote"].sum() / total_jobs * 100) if total_jobs > 0 else 0

for col, val, label in [
    (col1, f"{total_jobs:,}", "Total Jobs"),
    (col2, f"${avg_salary:,.0f}", "Avg Min Salary"),
    (col3, top_country, "Top Country"),
    (col4, f"{remote_pct:.1f}%", "Remote Jobs"),
]:
    col.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{val}</div>
        <div class="metric-label">{label}</div>
    </div>""", unsafe_allow_html=True)

st.divider()

# ── Row 1: Trend + Salary ─────────────────────────────────────
col_left, col_right = st.columns([3, 2])

with col_left:
    st.markdown('<div class="section-title">📈 Posting Volume Trend</div>', unsafe_allow_html=True)
    if not trend_df.empty:
        fig = px.area(
            trend_df, x="posted_date", y="job_count",
            color_discrete_sequence=["#4f8ef7"],
        )
        fig.update_traces(fill="tozeroy", fillcolor="rgba(79,142,247,0.15)")
        fig.update_layout(**PLOTLY_THEME)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No trend data available")

with col_right:
    st.markdown('<div class="section-title">💰 Avg Salary by Seniority</div>', unsafe_allow_html=True)
    if not sal_df.empty:
        fig = go.Figure()
        fig.add_bar(
            x=sal_df["seniority"], y=sal_df["avg_min"],
            name="Min", marker_color="#4f8ef7"
        )
        fig.add_bar(
            x=sal_df["seniority"], y=sal_df["avg_max"],
            name="Max", marker_color="#7c3aed"
        )
        fig.update_layout(barmode="group", **PLOTLY_THEME)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No salary data available")

# ── Row 2: Companies + Seniority pie ─────────────────────────
col_left2, col_right2 = st.columns([2, 1])

with col_left2:
    st.markdown('<div class="section-title">🏢 Top Companies Hiring</div>', unsafe_allow_html=True)
    if not comp_df.empty:
        fig = px.bar(
            comp_df.sort_values("job_count"),
            x="job_count", y="company",
            orientation="h",
            color="job_count",
            color_continuous_scale=["#1e2130", "#4f8ef7"],
        )
        fig.update_layout(**PLOTLY_THEME, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No company data available")

with col_right2:
    st.markdown('<div class="section-title">🎯 Seniority Mix</div>', unsafe_allow_html=True)
    if not jobs_df.empty and "seniority" in jobs_df.columns:
        sen_counts = jobs_df["seniority"].value_counts().reset_index()
        sen_counts.columns = ["seniority", "count"]
        fig = px.pie(
            sen_counts, values="count", names="seniority",
            color_discrete_sequence=["#4f8ef7", "#7c3aed", "#06b6d4"],
            hole=0.4,
        )
        fig.update_layout(**PLOTLY_THEME)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No seniority data available")

# ── Row 3: Jobs table ─────────────────────────────────────────
st.markdown('<div class="section-title">📋 Latest Job Postings</div>', unsafe_allow_html=True)
if not jobs_df.empty:
    display_cols = [c for c in ["title", "company", "country", "seniority", "salary_min", "is_remote", "posted_date", "redirect_url"] if c in jobs_df.columns]
    st.dataframe(
        jobs_df[display_cols].head(50),
        use_container_width=True,
        hide_index=True,
    )