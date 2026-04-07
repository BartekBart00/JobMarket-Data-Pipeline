import streamlit as st
import duckdb
import pandas as pd
from streamlit_echarts import st_echarts, JsCode

st.set_page_config(page_title="Data Jobs Pulse", page_icon="📈", layout="wide", initial_sidebar_state="expanded")

def render_no_data_info(accent_color="#3f51b5", text="No data available for current filters."):
    st.markdown(f"""
        <div style="background-color: #f8f9fa; padding: 24px; border-radius: 14px; border-left: 5px solid {accent_color}; display: flex; align-items: flex-start; gap: 18px; margin-top: 10px;">
            <div style="background-color: {accent_color}22; padding: 10px; border-radius: 10px;">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="{accent_color}"><path d="M11 15h2v2h-2v-2zm0-8h2v6h-2V7zm.99-5C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2z"/></svg>
            </div>
            <div>
                <b style="color: {accent_color}; font-size: 16px;">No Data Found</b><br/>
                <p style="color: #666; font-size: 14px; margin-top: 4px; line-height: 1.5;">
                    {text} Try adjusting the filters in the sidebar to expand your search.
                </p>
            </div>
        </div>
    """, unsafe_allow_html=True)

MAIN_COLOR = "#3f51b5"      
SECONDARY_COLOR = "#0277bd"  
ACCENT_COLOR = "#5e35b1"     
HEADER_COLOR = "#1a237e"     
BACKGROUND_COLOR = "#f4f7f9" 

EXP_LEVEL_MAP = {
    "junior": "Junior",
    "mid": "Mid",
    "senior": "Senior",
    "c_level": "C-Level",
}
EXP_LEVEL_REVERSE = {v: k for k, v in EXP_LEVEL_MAP.items()}

WP_TYPE_MAP = {
    "remote": "Remote",
    "hybrid": "Hybrid",
    "office": "Office",
}
WP_TYPE_REVERSE = {v: k for k, v in WP_TYPE_MAP.items()}

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Inter:wght@400;500;600&display=swap');
    header[data-testid="stHeader"] {
        display: none !important;
    }

    [data-testid="stSidebarCollapseButton"],
    [data-testid="collapsedControl"] {
        display: none !important;
    }
    [data-testid="stMain"] .block-container {
        padding-top: 1.5rem !important;
    }
    
    [data-testid="stSidebar"] [data-testid="stSidebarUserContent"],
    [data-testid="stSidebar"] > div:first-child {
        padding-top: 1.5rem !important;
    }

    [data-testid="stSidebarHeader"] {
        display: none !important;
        padding: 0 !important;
        height: 0 !important;
    }

    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif !important;
    }
    
    section[data-testid="stSidebar"] :not([data-testid="stSidebarCollapseButton"]) > span,
    section[data-testid="stMain"] span,
    p, div, h1, h2, h3, h4, h5, h6, li, a, label, input, textarea, select {
        font-family: 'Outfit', sans-serif !important;
    }
    
    .kpi-value, .chart-label {
        font-family: 'Inter', sans-serif !important;
    }
    section[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] span,
    section[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] span span,
    [data-testid="stSidebarCollapseButton"] span,
    [data-testid="stSidebarCollapseButton"] span span,
    button[kind="headerNoPadding"] span {
        font-family: 'Material Symbols Rounded', sans-serif !important;
    }

    .stApp {
        background-color: #f8fbfe;
    }

    section[data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e8e8e8;
    }
    section[data-testid="stSidebar"] .stMarkdown h2 {
        margin-bottom: 4px;
    }

    section[data-testid="stSidebar"] [data-testid="stMultiSelect"] {
        background-color: #ffffff;
        border-radius: 8px;
    }
    section[data-testid="stSidebar"] [data-testid="stMultiSelect"] > div {
        background-color: #ffffff;
        border: 1.5px solid #c8c8c8;
        border-radius: 8px;
    }
    section[data-testid="stSidebar"] [data-testid="stMultiSelect"] > div:focus-within {
        border-color: #3f51b5;
        box-shadow: 0 0 0 1px #3f51b5;
    }

    .filter-section {
        display: flex;
        align-items: center;
        gap: 10px;
        margin: 22px 0 6px 0;
        padding-bottom: 6px;
        border-bottom: 2px solid #e8eaf6;
    }
    .filter-section .icon {
        font-size: 15px;
        width: 28px;
        height: 28px;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 6px;
        background-color: #e8eaf6;
        color: #3f51b5;
        flex-shrink: 0;
        font-weight: 700;
    }
    .filter-section .label {
        font-size: 13px;
        font-weight: 600;
        color: #333;
        letter-spacing: 0.6px;
        text-transform: uppercase;
    }

    .kpi-card {
        position: relative;
        overflow: hidden;
        padding: 24px 24px 20px;
        border-radius: 14px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.06);
        transition: transform 0.25s ease, box-shadow 0.25s ease;
        cursor: default;
    }
    .kpi-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 24px rgba(0,0,0,0.12);
    }
    .kpi-card .watermark {
        position: absolute;
        right: -8px;
        bottom: -10px;
        opacity: 0.08;
    }
    .kpi-card .icon-badge {
        width: 42px;
        height: 42px;
        border-radius: 12px;
        display: flex;
        align-items: center;
        justify-content: center;
        margin-bottom: 14px;
    }
    .kpi-card .kpi-value {
        font-size: 34px;
        font-weight: 800;
        margin-bottom: 2px;
        line-height: 1.1;
    }
    .kpi-card .kpi-label {
        font-size: 13px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        opacity: 0.7;
    }

    .kpi-indigo { background: linear-gradient(135deg, #e8eaf6 0%, #c5cae9 100%); }
    .kpi-indigo .icon-badge { background-color: #9fa8da; }
    .kpi-indigo .kpi-value  { color: #1a237e; }
    .kpi-indigo .kpi-label  { color: #3f51b5; }

    .kpi-sapphire { background: linear-gradient(135deg, #e1f5fe 0%, #b3e5fc 100%); }
    .kpi-sapphire .icon-badge { background-color: #81d4fa; }
    .kpi-sapphire .kpi-value  { color: #01579b; }
    .kpi-sapphire .kpi-label  { color: #0277bd; }

    .kpi-slate { background: linear-gradient(135deg, #ede7f6 0%, #d1c4e9 100%); }
    .kpi-slate .icon-badge { background-color: #b39ddb; }
    .kpi-slate .kpi-value  { color: #311b92; }
    .kpi-slate .kpi-label  { color: #5e35b1; }

    h1, h2, h3 {
        color: #1a237e;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 4px;">
        <div style="display: flex; align-items: center; gap: 14px;">
            <svg width="36" height="36" viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg">
                <rect x="4" y="18" width="8" height="18" rx="4" fill="#3f51b5"/>
                <rect x="16" y="8" width="8" height="28" rx="4" fill="#0277bd"/>
                <rect x="28" y="14" width="8" height="22" rx="4" fill="#5e35b1"/>
            </svg>
            <h1 style="margin: 0; font-family: 'Outfit', sans-serif; font-size: 38px; font-weight: 800; color: #1a237e;">Data Jobs Pulse</h1>
        </div>
        <div style="color: #555; font-size: 13px; font-weight: 600; font-family: 'Inter', sans-serif; background-color: #e8eaf6; padding: 6px 14px; border-radius: 20px; display: flex; align-items: center; gap: 8px;">
            <span style="display: inline-block; width: 8px; height: 8px; background-color: #4caf50; border-radius: 50%; box-shadow: 0 0 4px rgba(76, 175, 80, 0.5);"></span>
            Last updated: April 2026
        </div>
    </div>
    <p style="color: #777; font-size: 15px; margin-top: 2px;">Real-time insights into the data job market in Poland - skills, salaries & trends at a glance.</p>
""", unsafe_allow_html=True)

@st.cache_resource
def get_db():
    return duckdb.connect("data/data_job_market.db", read_only=True)

conn = get_db()

@st.cache_data
def get_filter_options():
    exp_levels = conn.execute("SELECT DISTINCT experience_level FROM gold.dim_offers WHERE experience_level IS NOT NULL").df()['experience_level'].tolist()
    wp_types = conn.execute("SELECT DISTINCT workplace_type FROM gold.dim_offers WHERE workplace_type IS NOT NULL").df()['workplace_type'].tolist()
    comp_sizes = conn.execute("SELECT DISTINCT company_size FROM gold.dim_companies WHERE company_size IS NOT NULL ORDER BY company_size_sort_idx").df()['company_size'].tolist()
    titles = conn.execute("SELECT DISTINCT title FROM gold.dim_offers WHERE title IS NOT NULL ORDER BY title").df()['title'].tolist()
    
    salary_bounds = conn.execute("SELECT MIN(salary_avg), MAX(salary_avg) FROM gold.fct_offer_salaries WHERE salary_avg IS NOT NULL").df()
    min_sal = int(salary_bounds.iloc[0, 0]) if not pd.isna(salary_bounds.iloc[0, 0]) else 0
    max_sal = int(salary_bounds.iloc[0, 1]) if not pd.isna(salary_bounds.iloc[0, 1]) else 100000
    
    min_sal = (min_sal // 1000) * 1000
    max_sal = ((max_sal // 1000) + 1) * 1000

    return exp_levels, wp_types, comp_sizes, titles, min_sal, max_sal

exp_levels_raw, wp_types_raw, comp_sizes, titles, global_min_sal, global_max_sal = get_filter_options()

exp_levels_display = [EXP_LEVEL_MAP.get(v, v) for v in exp_levels_raw]
_exp_order = ["Junior", "Mid", "Senior", "C-Level"]
exp_levels_display = sorted(exp_levels_display, key=lambda x: _exp_order.index(x) if x in _exp_order else 999)
wp_types_display = [WP_TYPE_MAP.get(v, v) for v in wp_types_raw]

st.sidebar.markdown("""
    <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 0;">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M3 4.5h18v2.1l-6.9 7.5V20l-4.2 2V14.1L3 6.6V4.5Z" fill="#1a237e"/>
        </svg>
        <h2 style="color: #1a237e; margin: 0; font-size: 28px; font-weight: 700;">Filters</h2>
    </div>
""", unsafe_allow_html=True)

st.sidebar.markdown("""
    <div class="filter-section">
        <div class="icon">✦</div>
        <div class="label">Offer Title</div>
    </div>
""", unsafe_allow_html=True)
selected_title = st.sidebar.multiselect("Offer Title", options=titles, label_visibility="collapsed")

st.sidebar.markdown("""
    <div class="filter-section">
        <div class="icon">▸</div>
        <div class="label">Experience Level</div>
    </div>
""", unsafe_allow_html=True)
selected_exp_display = st.sidebar.multiselect("Experience Level", options=exp_levels_display, label_visibility="collapsed")
selected_exp = [EXP_LEVEL_REVERSE.get(v, v) for v in selected_exp_display]

st.sidebar.markdown("""
    <div class="filter-section">
        <div class="icon">■</div>
        <div class="label">Company Size</div>
    </div>
""", unsafe_allow_html=True)
selected_size = st.sidebar.multiselect("Company Size", options=comp_sizes, label_visibility="collapsed")

st.sidebar.markdown("""
    <div class="filter-section">
        <div class="icon">◎</div>
        <div class="label">Workplace Type</div>
    </div>
""", unsafe_allow_html=True)
selected_wpt_display = st.sidebar.multiselect("Workplace Type", options=wp_types_display, label_visibility="collapsed")
selected_wpt = [WP_TYPE_REVERSE.get(v, v) for v in selected_wpt_display]

st.sidebar.markdown("""
    <div class="filter-section">
        <div class="icon">$</div>
        <div class="label">Salary Range (PLN)</div>
    </div>
""", unsafe_allow_html=True)
selected_salary = st.sidebar.slider(
    "Salary Range",
    min_value=global_min_sal,
    max_value=global_max_sal,
    value=(global_min_sal, global_max_sal),
    step=1000,
    label_visibility="collapsed"
)

where_clauses = []
if selected_exp:
    formatted = [f"'{x}'" for x in selected_exp]
    where_clauses.append(f"o.experience_level IN ({','.join(formatted)})")
if selected_wpt:
    formatted = [f"'{x}'" for x in selected_wpt]
    where_clauses.append(f"o.workplace_type IN ({','.join(formatted)})")
if selected_size:
    formatted = [f"'{x}'" for x in selected_size]
    where_clauses.append(f"c.company_size IN ({','.join(formatted)})")
if selected_title:
    formatted = [f"'{x.replace(chr(39), chr(39)+chr(39))}'" for x in selected_title]
    where_clauses.append(f"o.title IN ({','.join(formatted)})")
if selected_salary != (global_min_sal, global_max_sal):
    min_s, max_s = selected_salary
    where_clauses.append(f"(m.max_salary_avg >= {min_s} AND m.max_salary_avg <= {max_s})")

where_sql = " AND ".join(where_clauses)
if where_sql:
    where_sql = "WHERE " + where_sql

BASE_CTE = f"""
    WITH offer_company_map AS (
        SELECT offer_key, company_key, MAX(salary_avg) as max_salary_avg
        FROM gold.fct_offer_salaries
        GROUP BY offer_key, company_key
    ),
    filtered_offers AS (
        SELECT o.offer_key, o.experience_level, o.workplace_type, m.company_key, o.title
        FROM gold.dim_offers o
        LEFT JOIN offer_company_map m ON o.offer_key = m.offer_key
        LEFT JOIN gold.dim_companies c ON m.company_key = c.company_key
        {where_sql}
    )
"""

def render_kpis():
    kpi_query = f"""
        {BASE_CTE}
        SELECT 
            COUNT(DISTINCT fo.offer_key) as total_offers,
            COUNT(DISTINCT fo.company_key) as total_companies,
            ROUND(AVG(s.salary_avg)) as avg_salary
        FROM filtered_offers fo
        LEFT JOIN gold.fct_offer_salaries s ON fo.offer_key = s.offer_key
    """
    kpis = conn.execute(kpi_query).df().iloc[0]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
            <div class="kpi-card kpi-indigo">
                <div class="icon-badge">
                    <svg width="22" height="22" viewBox="0 0 24 24" fill="#1a237e"><path d="M20 6H16V4C16 2.89 15.11 2 14 2H10C8.89 2 8 2.89 8 4V6H4C2.89 6 2 6.89 2 8V19C2 20.11 2.89 21 4 21H20C21.11 21 22 20.11 22 19V8C22 6.89 21.11 6 20 6ZM10 4H14V6H10V4ZM20 19H4V8H20V19Z"/></svg>
                </div>
                <div class="kpi-value">{int(kpis['total_offers']):,}</div>
                <div class="kpi-label">Total Offers</div>
                <div class="watermark">
                    <svg width="80" height="80" viewBox="0 0 24 24" fill="#1a237e"><path d="M20 6H16V4C16 2.89 15.11 2 14 2H10C8.89 2 8 2.89 8 4V6H4C2.89 6 2 6.89 2 8V19C2 20.11 2.89 21 4 21H20C21.11 21 22 20.11 22 19V8C22 6.89 21.11 6 20 6Z"/></svg>
                </div>
            </div>
        """, unsafe_allow_html=True)
        
    with col2:
        if pd.notna(kpis['avg_salary']):
            val = int(kpis['avg_salary'])
            formatted_val = f"{val:,} PLN"
            val_html = f'<div class="kpi-value">{formatted_val}</div>'
        else:
            val_html = '<div class="kpi-value" style="font-size: 16px; margin-top: 10px; line-height: 1.2; opacity: 0.8; color: #1a237e;">No salary data – adjust filters</div>'
            
        st.markdown(f"""
            <div class="kpi-card kpi-sapphire">
                <div class="icon-badge">
                    <svg width="22" height="22" viewBox="0 0 24 24" fill="#01579b"><path d="M11.8 10.9c-2.27-.59-3-1.2-3-2.15 0-1.09 1.01-1.85 2.7-1.85 1.78 0 2.44.85 2.5 2.1h2.21c-.07-1.72-1.12-3.3-3.21-3.81V3h-3v2.16c-1.94.42-3.5 1.68-3.5 3.61 0 2.31 1.91 3.46 4.7 4.13 2.5.6 3 1.48 3 2.41 0 .69-.49 1.79-2.7 1.79-2.06 0-2.87-.92-2.98-2.1h-2.2c.12 2.19 1.76 3.42 3.68 3.83V21h3v-2.15c1.95-.37 3.5-1.5 3.5-3.55 0-2.84-2.43-3.81-4.7-4.4Z"/></svg>
                </div>
                {val_html}
                <div class="kpi-label">Avg Salary (Month)</div>
                <div class="watermark">
                    <svg width="80" height="80" viewBox="0 0 24 24" fill="#01579b"><circle cx="12" cy="12" r="10"/></svg>
                </div>
            </div>
        """, unsafe_allow_html=True)
        
    with col3:
        st.markdown(f"""
            <div class="kpi-card kpi-slate">
                <div class="icon-badge">
                    <svg width="22" height="22" viewBox="0 0 24 24" fill="#311b92"><path d="M12 7V3H2v18h20V7H12ZM6 19H4v-2h2v2Zm0-4H4v-2h2v2Zm0-4H4V9h2v2Zm0-4H4V5h2v2Zm4 12H8v-2h2v2Zm0-4H8v-2h2v2Zm0-4H8V9h2v2Zm0-4H8V5h2v2Zm10 12h-8v-2h2v-2h-2v-2h2v-2h-2V9h8v10Zm-2-8h-2v2h2v-2Zm0 4h-2v2h2v-2Z"/></svg>
                </div>
                <div class="kpi-value">{int(kpis['total_companies']):,}</div>
                <div class="kpi-label">Hiring Companies</div>
                <div class="watermark">
                    <svg width="80" height="80" viewBox="0 0 24 24" fill="#311b92"><path d="M12 7V3H2v18h20V7H12Z"/></svg>
                </div>
            </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
render_kpis()
st.markdown("<br><hr>", unsafe_allow_html=True)

colA, colB = st.columns(2)

with colA:
    st.markdown("<h3 style='font-family: \"Outfit\", sans-serif; font-weight: 700; color: #1e293b; border-left: 4px solid #3f51b5; padding-left: 12px; margin-bottom: 20px;'>Top 10 In-Demand Skills</h3>", unsafe_allow_html=True)
    skills_query = f"""
        {BASE_CTE}
        SELECT ds.skill_name, COUNT(DISTINCT fo.offer_key) as count
        FROM filtered_offers fo
        JOIN gold.fct_offer_skills fos ON fo.offer_key = fos.offer_key
        JOIN gold.dim_skills ds ON fos.skill_key = ds.skill_key
        GROUP BY ds.skill_name
        ORDER BY count DESC
        LIMIT 10
    """
    df_skills = conn.execute(skills_query).df()
    total_filtered = conn.execute(f"{BASE_CTE} SELECT COUNT(DISTINCT offer_key) as n FROM filtered_offers").df().iloc[0]['n']

    df_skills = df_skills.sort_values(by="count", ascending=True).reset_index(drop=True)
    n = len(df_skills)

    gradient_colors = [
        "#c5cae9", "#9fa8da", "#7986cb", "#5c6bc0", "#3f51b5",
        "#3949ab", "#303f9f", "#283593", "#1a237e", "#0d1b60"
    ]

    bar_data = []
    for i, row in df_skills.iterrows():
        pct = round(row['count'] / total_filtered * 100, 1) if total_filtered > 0 else 0
        color_idx = min(i, len(gradient_colors) - 1)
        bar_data.append({
            "value": int(row['count']),
            "itemStyle": {
                "color": gradient_colors[color_idx],
                "borderRadius": [0, 6, 6, 0]
            },
        })

    rank_labels = []
    for i, name in enumerate(df_skills['skill_name'].tolist()):
        rank = n - i
        if rank == 1:
            rank_labels.append(f"🥇  {name}")
        elif rank == 2:
            rank_labels.append(f"🥈  {name}")
        elif rank == 3:
            rank_labels.append(f"🥉  {name}")
        else:
            rank_labels.append(name)

    label_data = []
    max_val = df_skills['count'].max() if len(df_skills) > 0 else 1
    for i, row in df_skills.iterrows():
        pct = round(row['count'] / total_filtered * 100, 1) if total_filtered > 0 else 0
        label_data.append({
            "value": int(row['count']),
            "label": {
                "show": True,
                "position": "right",
                "formatter": f"{int(row['count'])}  ({pct}%)",
                "fontSize": 12,
                "fontWeight": 600,
                "color": "#555",
            },
            "itemStyle": {"color": "transparent"},
        })

    if df_skills.empty:
        render_no_data_info(MAIN_COLOR, "We couldn't find any skills matching these criteria.")
    else:
        options_skills = {
            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
            "grid": {"left": "3%", "right": "14%", "bottom": "3%", "top": "3%", "containLabel": True},
            "xAxis": {"type": "value", "show": False},
            "yAxis": {
                "type": "category",
                "data": rank_labels,
                "axisLine": {"show": False},
                "axisTick": {"show": False},
                "axisLabel": {"fontSize": 13, "fontWeight": 600, "color": "#444"},
            },
            "series": [
                {
                    "name": "Offers",
                    "type": "bar",
                    "data": bar_data,
                    "barWidth": "60%",
                    "z": 2,
                },
                {
                    "name": "",
                    "type": "bar",
                    "data": label_data,
                    "barGap": "-100%",
                    "barWidth": "60%",
                    "z": 3,
                    "tooltip": {"show": False}
                }
            ],
            "animationDuration": 1000,
        }
        st_echarts(options=options_skills, height="420px")

with colB:
    st.markdown("<h3 style='font-family: \"Outfit\", sans-serif; font-weight: 700; color: #1e293b; border-left: 4px solid #3f51b5; padding-left: 12px; margin-bottom: 20px;'>Workplace Type Distribution</h3>", unsafe_allow_html=True)
    wp_query = f"""
        {BASE_CTE}
        SELECT workplace_type as name, COUNT(DISTINCT offer_key) as value
        FROM filtered_offers
        WHERE workplace_type IS NOT NULL
        GROUP BY workplace_type
    """
    df_wp = conn.execute(wp_query).df()
    df_wp['name'] = df_wp['name'].map(WP_TYPE_MAP).fillna(df_wp['name'])
    
    if df_wp.empty:
        render_no_data_info("#00897b", "No workplace layout data for this selection.")
    else:
        total_wp = df_wp['value'].sum() if not df_wp.empty else 1
        
        options_wp = {
            "tooltip": {
                "trigger": "item",
                "formatter": "{b}: <b>{c}</b> ({d}%)"
            },
            "legend": {"bottom": "0%", "left": "center", "itemGap": 15},
            "series": [
                {
                    "name": "Workplace",
                    "type": "pie",
                    "radius": ["25%", "75%"],
                    "avoidLabelOverlap": False,
                    "itemStyle": {
                        "borderRadius": 6,
                        "borderColor": "#fff",
                        "borderWidth": 2
                    },
                    "label": {
                        "show": True,
                        "position": "inside",
                        "formatter": JsCode("function(params) { return params.percent >= 7 ? Math.round(params.percent) + '%' : ''; }"),
                        "color": "#fff",
                        "fontSize": 14,
                        "fontWeight": 700
                    },
                    "labelLine": {"show": False},
                    "emphasis": {
                        "scale": True,
                        "itemStyle": {
                            "shadowBlur": 10,
                            "shadowOffsetX": 0,
                            "shadowColor": "rgba(0, 0, 0, 0.5)"
                        }
                    },
                    "data": df_wp.to_dict(orient="records"),
                    "color": ["#1a237e", "#3f51b5", "#0277bd", "#5e35b1", "#455a64"]
                }
            ]
        }
        st_echarts(options=options_wp, height="420px")

st.markdown("<hr>", unsafe_allow_html=True)



t_col_a, t_col_b = st.columns(2)
with t_col_a:
    st.markdown("<h3 style='font-family: \"Outfit\", sans-serif; font-weight: 700; color: #1e293b; border-left: 4px solid #3f51b5; padding-left: 12px; margin: 0;'>Tech Stack DNA (Role vs Skill %)</h3>", unsafe_allow_html=True)
with t_col_b:
    st.markdown("<h3 style='font-family: \"Outfit\", sans-serif; font-weight: 700; color: #1e293b; border-left: 4px solid #3f51b5; padding-left: 12px; margin: 0;'>Avg Salary by Experience</h3>", unsafe_allow_html=True)

st.markdown("<div style='margin-bottom: 25px;'></div>", unsafe_allow_html=True)

col_heat, col_tree = st.columns(2)

with col_heat:
    titles_query = f"{BASE_CTE} SELECT title, COUNT(*) as n FROM filtered_offers GROUP BY title ORDER BY n DESC LIMIT 10"
    top_titles = conn.execute(titles_query).df()['title'].tolist()
    
    skills_query = f"""
        {BASE_CTE} 
        SELECT ds.skill_name, COUNT(*) as n 
        FROM filtered_offers fo
        JOIN gold.fct_offer_skills fos ON fo.offer_key = fos.offer_key
        JOIN gold.dim_skills ds ON fos.skill_key = ds.skill_key
        GROUP BY ds.skill_name ORDER BY n DESC LIMIT 10
    """
    top_skills = conn.execute(skills_query).df()['skill_name'].tolist()
    
    if not top_titles or not top_skills:
        render_no_data_info("#1a237e", "Not enough data to generate the Tech Stack DNA matrix.")
    else:
        matrix_query = f"""
            {BASE_CTE}
            SELECT fo.title, ds.skill_name, COUNT(*) as count
            FROM filtered_offers fo
            JOIN gold.fct_offer_skills fos ON fo.offer_key = fos.offer_key
            JOIN gold.dim_skills ds ON fos.skill_key = ds.skill_key
            WHERE fo.title IN ({', '.join([f"'{t}'" for t in top_titles])})
              AND ds.skill_name IN ({', '.join([f"'{s}'" for s in top_skills])})
            GROUP BY fo.title, ds.skill_name
        """
        df_matrix = conn.execute(matrix_query).df()
        
        title_totals_query = f"{BASE_CTE} SELECT title, COUNT(*) as total FROM filtered_offers WHERE title IN ({', '.join([f"'{t}'" for t in top_titles])}) GROUP BY title"
        df_totals = conn.execute(title_totals_query).df().set_index('title')
        
        heatmap_data = []
        for i, skill in enumerate(top_skills):
            for j, title in enumerate(top_titles):
                match = df_matrix[(df_matrix['title'] == title) & (df_matrix['skill_name'] == skill)]
                count = match['count'].iloc[0] if not match.empty else 0
                total = df_totals.loc[title, 'total'] if title in df_totals.index else 1
                pct = round((count / total) * 100, 1)
                heatmap_data.append([i, j, pct])
        
        options_heatmap = {
            "tooltip": {"position": "top", "formatter": JsCode("function(p){return '<b>' + p.name + '</b><br/>' + p.value[2] + '% of roles require this skill';}")},
            "grid": {"height": "75%", "top": "5%", "right": "5%", "bottom": "15%"},
            "xAxis": {"type": "category", "data": top_skills, "splitArea": {"show": True}, "axisLabel": {"rotate": 45, "fontSize": 10}},
            "yAxis": {"type": "category", "data": top_titles, "splitArea": {"show": True}, "axisLabel": {"fontSize": 10}},
            "visualMap": {
                "min": 0, "max": 100, "calculable": True, "orient": "horizontal", "left": "center", "bottom": "0%",
                "inRange": {"color": ["#f5f7f9", "#7986cb", "#1a237e"]},
                "text": ["100%", "0%"]
            },
            "series": [{"name": "Skill Match", "type": "heatmap", "data": heatmap_data, "label": {"show": True, "fontSize": 9}, "emphasis": {"itemStyle": {"shadowBlur": 10, "shadowColor": "rgba(0, 0, 0, 0.5)"}}}]
        }
        st_echarts(options=options_heatmap, height="500px")

with col_tree:
    salary_query = f"""
        {BASE_CTE}
        SELECT fo.experience_level, ROUND(AVG(s.salary_avg)) as avg_salary, COUNT(DISTINCT fo.offer_key) as offer_count
        FROM filtered_offers fo
        JOIN gold.fct_offer_salaries s ON fo.offer_key = s.offer_key
        WHERE fo.experience_level IS NOT NULL
        GROUP BY fo.experience_level
        ORDER BY avg_salary ASC
    """
    df_salary = conn.execute(salary_query).df()
    df_salary = df_salary.dropna(subset=['avg_salary'])
    df_salary['experience_level'] = df_salary['experience_level'].map(EXP_LEVEL_MAP).fillna(df_salary['experience_level'])

    if df_salary.empty:
        render_no_data_info("#e65100", "Insufficient salary data.")
    else:
        salary_data = []
        for _, row in df_salary.iterrows():
            salary_data.append({
                "value": int(row['avg_salary']),
                "offers": int(row['offer_count'])
            })

        options_salary = {
            "tooltip": {
                "trigger": "axis",
                "axisPointer": {"type": "shadow"},
                "formatter": JsCode("""function (params) {
                    var d = params[0].data;
                    return '<b>' + params[0].name + '</b><br/>' +
                           'Avg Salary: <b>' + d.value.toLocaleString() + ' PLN</b><br/>' +
                           '<span style="color:#777; font-size:12px">Based on ' + d.offers + ' offers</span>';
                }""")
            },
            "grid": {"left": "3%", "right": "4%", "bottom": "15%", "top": "12%", "containLabel": True},
            "xAxis": {
                "type": "category",
                "data": df_salary['experience_level'].tolist(),
                "axisTick": {"show": False},
                "axisLine": {"lineStyle": {"color": "#eee"}},
                "axisLabel": {"fontSize": 11, "fontWeight": 600, "color": "#444", "margin": 10, "rotate": 0}
            },
            "yAxis": {
                "type": "value",
                "name": "", 
                "splitLine": {"lineStyle": {"type": "dashed", "color": "#f0f0f0"}},
                "axisLabel": {"fontSize": 10, "color": "#999"}
            },
            "series": [
                {
                    "name": "Avg Salary",
                    "type": "bar",
                    "barWidth": "55%",
                    "data": salary_data,
                    "label": {
                        "show": True,
                        "position": "top",
                        "formatter": "{c} PLN",
                        "fontSize": 11,
                        "fontWeight": "bold",
                        "color": "#3f51b5",
                        "distance": 10
                    },
                    "itemStyle": {
                        "color": {
                            "type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
                            "colorStops": [
                                {"offset": 0, "color": "#3f51b5"},
                                {"offset": 1, "color": "#7986cb"}
                            ]
                        },
                        "borderRadius": [6, 6, 0, 0]
                    }
                }
            ]
        }
        st_echarts(options=options_salary, height="500px")


st.markdown("<hr>", unsafe_allow_html=True)

st.markdown("<h3 style='font-family: \"Outfit\", sans-serif; font-weight: 700; color: #1e293b; border-left: 4px solid #3f51b5; padding-left: 12px; margin-bottom: 20px;'>Role & Experience Level Breakdown (Top 10)</h3>", unsafe_allow_html=True)

role_exp_query = f"""
    {BASE_CTE}
    SELECT fo.title, fo.experience_level, COUNT(*) as count
    FROM filtered_offers fo
    WHERE fo.title IS NOT NULL AND fo.experience_level IS NOT NULL
    GROUP BY fo.title, fo.experience_level
"""
df_role_exp = conn.execute(role_exp_query).df()

if df_role_exp.empty:
    render_no_data_info("#1a237e", "No role/level breakdown available for current filters.")
else:
    role_totals = df_role_exp.groupby('title')['count'].sum().nlargest(10)
    top_10_roles = role_totals.index.tolist()
    df_role_exp_top = df_role_exp[df_role_exp['title'].isin(top_10_roles)]
    pivot_df = df_role_exp_top.pivot_table(index='title', columns='experience_level', values='count', fill_value=0)
    
    for level in EXP_LEVEL_MAP.keys():
        if level not in pivot_df.columns:
            pivot_df[level] = 0
            
    pivot_df['total_for_sort'] = pivot_df.sum(axis=1)
    pivot_df = pivot_df.sort_values(by='total_for_sort', ascending=True)
    titles_list = pivot_df.index.tolist()
    
    LEVEL_COLORS = {
        "junior": "#c5cae9",
        "mid": "#7986cb",
        "senior": "#3f51b5",
        "c_level": "#1a237e"
    }
    
    series = []
    order = ["junior", "mid", "senior", "c_level"]
    for level in order:
        if level in pivot_df.columns:
            series.append({
                "name": EXP_LEVEL_MAP.get(level, level),
                "type": "bar",
                "stack": "total",
                "label": {"show": False},
                "emphasis": {"focus": "series"},
                "itemStyle": {"color": LEVEL_COLORS.get(level, "#ccc")},
                "data": pivot_df[level].tolist()
            })

    options_role_exp = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "legend": {"bottom": "0%", "left": "center"},
        "grid": {"left": "3%", "right": "4%", "bottom": "12%", "top": "5%", "containLabel": True},
        "xAxis": {"type": "value", "splitLine": {"show": False}},
        "yAxis": {
            "type": "category", 
            "data": titles_list,
            "axisLabel": {"fontSize": 12, "fontWeight": 600, "color": "#444"}
        },
        "series": series
    }
    st_echarts(options=options_role_exp, height="500px")

