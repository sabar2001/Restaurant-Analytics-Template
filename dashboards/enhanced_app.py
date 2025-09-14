#!/usr/bin/env python3
"""
Enhanced Restaurant Analytics Dashboard
Features: Market Intelligence, Competitive Analysis, Investment Insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import os
import sys

# Add the parent directory to the path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Helper function for consistent chart theming
def apply_dark_theme(fig):
    """Apply consistent dark theme to plotly charts."""
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='#e6edf3',
        title_font_color='#f0f6fc',
        showlegend=True,
        title_font_size=16,
        title_x=0.5,
        legend=dict(
            bgcolor='rgba(0,0,0,0.3)',
            bordercolor='#30363d',
            borderwidth=1
        )
    )
    fig.update_xaxes(gridcolor='#21262d', linecolor='#30363d')
    fig.update_yaxes(gridcolor='#21262d', linecolor='#30363d')
    return fig

# Page configuration
st.set_page_config(
    page_title="Restaurant Market Intelligence",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Modern dark theme CSS
st.markdown("""
<style>
    .stApp {
        background-color: #0d1117;
        color: #e6edf3;
    }
    
    .main-header {
        font-size: 3rem;
        color: #58a6ff;
        font-weight: 600;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #58a6ff, #7c3aed);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #161b22, #21262d);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 0.5rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    
    .insight-box {
        background: linear-gradient(135deg, #1a1f36, #2d3748);
        border-left: 4px solid #58a6ff;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 8px 8px 0;
    }
    
    .competitive-advantage {
        background: linear-gradient(135deg, #1a3d1a, #2d5a2d);
        border-left: 4px solid #22c55e;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 8px 8px 0;
    }
    
    .market-warning {
        background: linear-gradient(135deg, #3d1a1a, #5a2d2d);
        border-left: 4px solid #ef4444;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 8px 8px 0;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">🍽️ Restaurant Market Intelligence</h1>', unsafe_allow_html=True)

# Load real data from BigQuery
@st.cache_data
def load_restaurant_data():
    """Load real restaurant data from BigQuery with error handling."""
    try:
        from google.cloud import bigquery
        from ingestion.utils import get_bigquery_config
        
        config = get_bigquery_config()
        if not config:
            return None, "BigQuery configuration missing"
        
        client = bigquery.Client(project=config.get('project_id'))
        
        # Enhanced query with business intelligence focus
        query = f"""
        WITH restaurant_metrics AS (
          SELECT 
            place_id,
            name,
            rating,
            user_ratings_total as review_count,
            price_level,
            types,
            formatted_address,
            geometry_location_lat as latitude,
            geometry_location_lng as longitude,
            data_source,
            SPLIT(formatted_address, ', ')[SAFE_OFFSET(1)] as city,
            SPLIT(formatted_address, ', ')[SAFE_OFFSET(2)] as state,
            
            -- Business Intelligence Metrics
            CASE 
              WHEN rating >= 4.5 AND user_ratings_total >= 100 THEN 'Market Leader'
              WHEN rating >= 4.2 AND user_ratings_total >= 50 THEN 'Strong Performer'
              WHEN rating >= 3.8 AND user_ratings_total >= 25 THEN 'Average Performer'
              WHEN rating >= 3.5 THEN 'Below Average'
              ELSE 'Poor Performance'
            END as performance_tier,
            
            CASE 
              WHEN price_level = 1 THEN 'Budget ($)'
              WHEN price_level = 2 THEN 'Mid-Range ($$)'
              WHEN price_level = 3 THEN 'Upscale ($$$)'
              WHEN price_level = 4 THEN 'Fine Dining ($$$$)'
              ELSE 'Unknown'
            END as price_category,
            
            -- Market Position Score (0-100)
            LEAST(100, 
              (rating - 1) * 20 +  -- Rating component (0-80)
              LEAST(20, LOG10(GREATEST(1, user_ratings_total)) * 4) -- Review volume component (0-20)
            ) as market_score,
            
            -- Revenue Potential (rough estimate)
            CASE 
              WHEN price_level >= 3 AND rating >= 4.0 THEN 'High Revenue Potential'
              WHEN price_level >= 2 AND rating >= 3.8 THEN 'Medium Revenue Potential'
              ELSE 'Low Revenue Potential'
            END as revenue_potential,
            
            ingestion_timestamp
            
          FROM `{config.get('project_id')}.{config.get('dataset_id')}.raw_restaurant_data`
          WHERE rating IS NOT NULL
        )
        SELECT * FROM restaurant_metrics
        ORDER BY market_score DESC
        """
        
        df = client.query(query).to_dataframe()
        return df, None
        
    except Exception as e:
        return None, str(e)

# Load data
df, error = load_restaurant_data()

if error:
    st.error(f"⚠️ Could not load data: {error}")
    st.info("💡 Make sure BigQuery is configured and the pipeline has been run")
    st.stop()

if df is None or df.empty:
    st.warning("📊 No restaurant data found. Please run the data pipeline first.")
    st.stop()

# Sidebar filters
st.sidebar.markdown("### 🎯 Market Analysis Filters")

# City filter
cities = sorted(df['city'].dropna().unique())
selected_cities = st.sidebar.multiselect(
    "📍 Markets to Analyze",
    options=cities,
    default=cities[:5] if len(cities) > 5 else cities,
    help="Select cities for competitive analysis"
)

# Performance tier filter
performance_tiers = df['performance_tier'].unique()
selected_tiers = st.sidebar.multiselect(
    "🏆 Performance Segments",
    options=performance_tiers,
    default=performance_tiers,
    help="Filter by business performance level"
)

# Price segment filter
price_segments = df['price_category'].unique()
selected_price = st.sidebar.multiselect(
    "💰 Price Segments",
    options=price_segments,
    default=price_segments,
    help="Analyze specific price points"
)

# Apply filters
filtered_df = df[
    (df['city'].isin(selected_cities)) &
    (df['performance_tier'].isin(selected_tiers)) &
    (df['price_category'].isin(selected_price))
]

# Main dashboard
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric(
        "🏪 Total Restaurants",
        f"{len(filtered_df):,}",
        delta=f"Across {len(selected_cities)} markets"
    )
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    avg_score = filtered_df['market_score'].mean()
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric(
        "📊 Avg Market Score",
        f"{avg_score:.1f}/100",
        delta=f"Industry standard: 65"
    )
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    market_leaders = len(filtered_df[filtered_df['performance_tier'] == 'Market Leader'])
    market_share = (market_leaders / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric(
        "👑 Market Leaders",
        f"{market_leaders:,} ({market_share:.1f}%)",
        delta="High-performing restaurants"
    )
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    high_revenue = len(filtered_df[filtered_df['revenue_potential'] == 'High Revenue Potential'])
    revenue_percentage = (high_revenue / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric(
        "💎 High Revenue Potential",
        f"{high_revenue:,} ({revenue_percentage:.1f}%)",
        delta="Investment opportunities"
    )
    st.markdown('</div>', unsafe_allow_html=True)

# Business Intelligence Section
st.markdown("---")
st.markdown("## 🧠 Market Intelligence")

col1, col2 = st.columns(2)

with col1:
    # Market Position Analysis
    market_analysis = filtered_df.groupby(['city', 'performance_tier']).size().reset_index(name='count')
    fig_market = px.bar(
        market_analysis,
        x='city',
        y='count',
        color='performance_tier',
        title='Market Competitive Landscape by City',
        color_discrete_map={
            'Market Leader': '#22c55e',
            'Strong Performer': '#3b82f6',
            'Average Performer': '#f59e0b',
            'Below Average': '#ef4444',
            'Poor Performance': '#6b7280'
        }
    )
    fig_market = apply_dark_theme(fig_market)
    st.plotly_chart(fig_market, width='stretch')

with col2:
    # Revenue Potential by Market
    revenue_analysis = filtered_df.groupby(['city', 'revenue_potential']).size().reset_index(name='count')
    fig_revenue = px.bar(
        revenue_analysis,
        x='city',
        y='count',
        color='revenue_potential',
        title='Revenue Potential Distribution by Market',
        color_discrete_map={
            'High Revenue Potential': '#22c55e',
            'Medium Revenue Potential': '#f59e0b',
            'Low Revenue Potential': '#6b7280'
        }
    )
    fig_revenue = apply_dark_theme(fig_revenue)
    st.plotly_chart(fig_revenue, width='stretch')

# Competitive Analysis
st.markdown("## ⚔️ Competitive Analysis")

col1, col2 = st.columns(2)

with col1:
    # Market Score Distribution
    fig_score = px.histogram(
        filtered_df,
        x='market_score',
        nbins=20,
        title='Market Score Distribution',
        color_discrete_sequence=['#58a6ff']
    )
    fig_score.add_vline(x=filtered_df['market_score'].mean(), line_dash="dash", 
                       annotation_text=f"Average: {filtered_df['market_score'].mean():.1f}")
    fig_score = apply_dark_theme(fig_score)
    st.plotly_chart(fig_score, width='stretch')

with col2:
    # Price vs Performance Scatter
    fig_scatter = px.scatter(
        filtered_df,
        x='rating',
        y='review_count',
        color='price_category',
        size='market_score',
        title='Price Point vs Performance Matrix',
        hover_data=['name', 'city', 'market_score']
    )
    fig_scatter = apply_dark_theme(fig_scatter)
    st.plotly_chart(fig_scatter, width='stretch')

# Market Insights
st.markdown("## 💡 Strategic Insights")

# Calculate insights
top_city = filtered_df.groupby('city')['market_score'].mean().sort_values(ascending=False).index[0]
top_city_score = filtered_df.groupby('city')['market_score'].mean().sort_values(ascending=False).iloc[0]

market_leaders = filtered_df[filtered_df['performance_tier'] == 'Market Leader']
underperformers = filtered_df[filtered_df['performance_tier'].isin(['Below Average', 'Poor Performance'])]

col1, col2 = st.columns(2)

with col1:
    st.markdown(f"""
    <div class="competitive-advantage">
    <h4>🎯 Market Opportunities</h4>
    <p><strong>Top Market:</strong> {top_city} (Score: {top_city_score:.1f})</p>
    <p><strong>Market Leaders:</strong> {len(market_leaders)} restaurants setting the standard</p>
    <p><strong>Growth Potential:</strong> {len(underperformers)} restaurants need improvement</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    # Price point analysis
    best_price_point = filtered_df.groupby('price_category')['market_score'].mean().sort_values(ascending=False).index[0]
    best_price_score = filtered_df.groupby('price_category')['market_score'].mean().sort_values(ascending=False).iloc[0]
    
    st.markdown(f"""
    <div class="insight-box">
    <h4>💰 Investment Intelligence</h4>
    <p><strong>Best Performing Segment:</strong> {best_price_point}</p>
    <p><strong>Average Score:</strong> {best_price_score:.1f}/100</p>
    <p><strong>High Revenue Opportunities:</strong> {high_revenue} restaurants</p>
    </div>
    """, unsafe_allow_html=True)

# Top Performers Table
st.markdown("## 🏆 Market Leaders & Investment Targets")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 👑 Top Market Leaders")
    top_performers = filtered_df[filtered_df['performance_tier'] == 'Market Leader'].head(10)
    if not top_performers.empty:
        display_top = top_performers[['name', 'city', 'rating', 'review_count', 'market_score']].copy()
        display_top['rating'] = display_top['rating'].round(1)
        display_top['market_score'] = display_top['market_score'].round(1)
        st.dataframe(
            display_top,
            column_config={
                "name": "Restaurant",
                "city": "Market",
                "rating": "Rating",
                "review_count": "Reviews",
                "market_score": "Score"
            },
            width='stretch'
        )

with col2:
    st.markdown("### 💎 High Revenue Potential")
    investment_targets = filtered_df[
        (filtered_df['revenue_potential'] == 'High Revenue Potential') &
        (filtered_df['performance_tier'].isin(['Market Leader', 'Strong Performer']))
    ].head(10)
    
    if not investment_targets.empty:
        display_investment = investment_targets[['name', 'city', 'price_category', 'market_score']].copy()
        display_investment['market_score'] = display_investment['market_score'].round(1)
        st.dataframe(
            display_investment,
            column_config={
                "name": "Restaurant",
                "city": "Market",
                "price_category": "Segment",
                "market_score": "Score"
            },
            width='stretch'
        )

# Market Warnings
if len(underperformers) > len(filtered_df) * 0.3:
    st.markdown(f"""
    <div class="market-warning">
    <h4>⚠️ Market Alert</h4>
    <p>High concentration of underperforming restaurants ({len(underperformers)}/{len(filtered_df)}).</p>
    <p>This indicates potential market saturation or operational challenges.</p>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #7d8590; padding: 1rem; font-size: 0.9rem;'>
        🍽️ Restaurant Market Intelligence | Powered by Real-Time Analytics
    </div>
    """,
    unsafe_allow_html=True
)
