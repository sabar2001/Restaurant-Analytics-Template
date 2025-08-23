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

# Page configuration
st.set_page_config(
    page_title="Restaurant Analytics Dashboard",
    page_icon="🍽️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
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
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">🍽️ Restaurant Analytics Dashboard</h1>', unsafe_allow_html=True)

# Sidebar for filters
st.sidebar.header("📊 Dashboard Filters")

# Mock data for demonstration (in real app, this would come from Snowflake)
@st.cache_data
def load_sample_data():
    """Load sample restaurant data for demonstration."""
    np.random.seed(42)
    
    # Generate sample data
    n_restaurants = 100
    
    data = {
        'restaurant_id': [f'REST_{i:03d}' for i in range(1, n_restaurants + 1)],
        'restaurant_name': [f'Restaurant {i}' for i in range(1, n_restaurants + 1)],
        'rating': np.random.normal(3.8, 0.8, n_restaurants).clip(1, 5),
        'rating_category': np.random.choice(['Excellent', 'Very Good', 'Good', 'Average', 'Below Average'], n_restaurants, p=[0.1, 0.2, 0.4, 0.2, 0.1]),
        'data_source': np.random.choice(['yelp', 'google_places'], n_restaurants),
        'city': np.random.choice(['San Francisco', 'New York', 'Los Angeles', 'Chicago', 'Miami'], n_restaurants),
        'state': np.random.choice(['CA', 'NY', 'LA', 'IL', 'FL'], n_restaurants),
        'price_level': np.random.choice([1, 2, 3, 4], n_restaurants, p=[0.3, 0.4, 0.2, 0.1]),
        'price_category': np.random.choice(['$', '$$', '$$$', '$$$$'], n_restaurants, p=[0.3, 0.4, 0.2, 0.1]),
        'review_count': np.random.poisson(50, n_restaurants),
        'review_volume_category': np.random.choice(['High Volume', 'Medium Volume', 'Low Volume', 'Very Low Volume'], n_restaurants, p=[0.2, 0.3, 0.3, 0.2]),
        'business_tier': np.random.choice(['Premium', 'High Quality', 'Good Quality', 'Standard', 'Needs Improvement'], n_restaurants, p=[0.1, 0.2, 0.3, 0.3, 0.1]),
        'latitude': np.random.uniform(37.7, 37.8, n_restaurants),
        'longitude': np.random.uniform(-122.5, -122.4, n_restaurants),
        'ingestion_timestamp': pd.date_range(start='2024-01-01', periods=n_restaurants, freq='D'),
        'processed_at': pd.Timestamp.now()
    }
    
    return pd.DataFrame(data)

# Load data
df = load_sample_data()

# Filters
st.sidebar.subheader("📍 Location Filter")
selected_cities = st.sidebar.multiselect(
    "Select Cities",
    options=df['city'].unique(),
    default=df['city'].unique()[:3]
)

st.sidebar.subheader("⭐ Rating Filter")
min_rating = st.sidebar.slider("Minimum Rating", 1.0, 5.0, 3.0, 0.1)

st.sidebar.subheader("💰 Price Filter")
selected_price_levels = st.sidebar.multiselect(
    "Select Price Levels",
    options=df['price_category'].unique(),
    default=df['price_category'].unique()
)

st.sidebar.subheader("📊 Data Source")
selected_sources = st.sidebar.multiselect(
    "Select Data Sources",
    options=df['data_source'].unique(),
    default=df['data_source'].unique()
)

# Apply filters
filtered_df = df[
    (df['city'].isin(selected_cities)) &
    (df['rating'] >= min_rating) &
    (df['price_category'].isin(selected_price_levels)) &
    (df['data_source'].isin(selected_sources))
]

# Main content
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Restaurants",
        value=len(filtered_df),
        delta=len(filtered_df) - len(df)
    )

with col2:
    avg_rating = filtered_df['rating'].mean()
    st.metric(
        label="Average Rating",
        value=f"{avg_rating:.2f}",
        delta=f"{avg_rating - df['rating'].mean():.2f}"
    )

with col3:
    total_reviews = filtered_df['review_count'].sum()
    st.metric(
        label="Total Reviews",
        value=f"{total_reviews:,}",
        delta=f"{total_reviews - df['review_count'].sum():,}"
    )

with col4:
    premium_count = len(filtered_df[filtered_df['business_tier'] == 'Premium'])
    st.metric(
        label="Premium Restaurants",
        value=premium_count,
        delta=premium_count - len(df[df['business_tier'] == 'Premium'])
    )

# Charts
st.markdown("---")
st.subheader("📈 Analytics Overview")

# Create two columns for charts
col1, col2 = st.columns(2)

with col1:
    # Rating distribution
    fig_rating = px.histogram(
        filtered_df,
        x='rating',
        nbins=20,
        title='Rating Distribution',
        color_discrete_sequence=['#1f77b4']
    )
    fig_rating.update_layout(showlegend=False)
    st.plotly_chart(fig_rating, use_container_width=True)

with col2:
    # Rating by city
    city_ratings = filtered_df.groupby('city')['rating'].mean().sort_values(ascending=False)
    fig_city = px.bar(
        x=city_ratings.index,
        y=city_ratings.values,
        title='Average Rating by City',
        color_discrete_sequence=['#ff7f0e']
    )
    fig_city.update_layout(showlegend=False)
    st.plotly_chart(fig_city, use_container_width=True)

# Price level analysis
st.subheader("💰 Price Level Analysis")

col1, col2 = st.columns(2)

with col1:
    # Price level distribution
    price_counts = filtered_df['price_category'].value_counts()
    fig_price = px.pie(
        values=price_counts.values,
        names=price_counts.index,
        title='Restaurant Distribution by Price Level'
    )
    st.plotly_chart(fig_price, use_container_width=True)

with col2:
    # Rating vs Price Level
    price_rating = filtered_df.groupby('price_category')['rating'].mean().sort_index()
    fig_price_rating = px.bar(
        x=price_rating.index,
        y=price_rating.values,
        title='Average Rating by Price Level',
        color_discrete_sequence=['#2ca02c']
    )
    fig_price_rating.update_layout(showlegend=False)
    st.plotly_chart(fig_price_rating, use_container_width=True)

# Data source comparison
st.subheader("📊 Data Source Comparison")

col1, col2 = st.columns(2)

with col1:
    # Data source distribution
    source_counts = filtered_df['data_source'].value_counts()
    fig_source = px.pie(
        values=source_counts.values,
        names=source_counts.index,
        title='Data Distribution by Source'
    )
    st.plotly_chart(fig_source, use_container_width=True)

with col2:
    # Rating by data source
    source_rating = filtered_df.groupby('data_source')['rating'].mean()
    fig_source_rating = px.bar(
        x=source_rating.index,
        y=source_rating.values,
        title='Average Rating by Data Source',
        color_discrete_sequence=['#d62728']
    )
    fig_source_rating.update_layout(showlegend=False)
    st.plotly_chart(fig_source_rating, use_container_width=True)

# Business tier analysis
st.subheader("🏆 Business Tier Analysis")

tier_counts = filtered_df['business_tier'].value_counts()
fig_tier = px.bar(
    x=tier_counts.index,
    y=tier_counts.values,
    title='Restaurant Distribution by Business Tier',
    color_discrete_sequence=['#9467bd']
)
fig_tier.update_layout(showlegend=False)
st.plotly_chart(fig_tier, use_container_width=True)

# Data table
st.markdown("---")
st.subheader("📋 Detailed Data View")

# Add search functionality
search_term = st.text_input("🔍 Search restaurants by name:")

if search_term:
    search_df = filtered_df[filtered_df['restaurant_name'].str.contains(search_term, case=False, na=False)]
else:
    search_df = filtered_df

# Display the data
st.dataframe(
    search_df[['restaurant_name', 'city', 'rating', 'price_category', 'review_count', 'business_tier', 'data_source']],
    use_container_width=True
)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 1rem;'>
        🍽️ Restaurant Analytics Dashboard | Built with Streamlit, dbt, and Snowflake
    </div>
    """,
    unsafe_allow_html=True
)
