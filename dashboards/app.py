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

from ingestion.location_manager import get_enabled_locations

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

# Location Management Section
st.sidebar.subheader("🗺️ Location Management")
if st.sidebar.button("View All Locations"):
    try:
        locations = get_enabled_locations()
        if locations:
            st.sidebar.success(f"✅ {len(locations)} locations configured")
            for loc in locations[:5]:  # Show first 5
                st.sidebar.text(f"• {loc}")
            if len(locations) > 5:
                st.sidebar.text(f"... and {len(locations) - 5} more")
        else:
            st.sidebar.warning("No locations configured")
    except Exception as e:
        st.sidebar.error(f"Error: {e}")

st.sidebar.markdown("---")

# Data loading functions
@st.cache_data
def load_real_data():
    """Load real restaurant data from BigQuery."""
    try:
        # Import BigQuery and utils
        from google.cloud import bigquery
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from ingestion.utils import get_bigquery_config
        
        # Get BigQuery configuration
        config = get_bigquery_config()
        if not config:
            st.error("BigQuery configuration not found. Check your .env file.")
            return pd.DataFrame()
        
        # Initialize BigQuery client
        client = bigquery.Client(
            project=config.get('project_id'),
            location=config.get('location')
        )
        
        # Query to get restaurant data (limited to 500 for performance)
        query = f"""
        SELECT 
            place_id,
            restaurant_name as name,
            rating,
            review_count,
            price_level,
            price_category,
            business_tier,
            rating_category,
            city,
            state,
            formatted_address,
            latitude,
            longitude,
            data_source,
            ingestion_timestamp,
            processed_at
        FROM `{config.get('project_id')}.{config.get('dataset_id')}.raw_restaurant_data`
        ORDER BY rating DESC, review_count DESC
        LIMIT 500
        """
        
        # Execute query and convert to DataFrame
        df = client.query(query).to_dataframe()
        
        if not df.empty:
            # Add some derived columns for better dashboard display
            df['review_volume_category'] = pd.cut(
                df['review_count'], 
                bins=[0, 10, 50, 200, float('inf')],
                labels=['Very Low Volume', 'Low Volume', 'Medium Volume', 'High Volume']
            )
            
            st.success(f"✅ Loaded {len(df)} real restaurants from BigQuery!")
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error loading real data from BigQuery: {e}")
        st.info("💡 Make sure you've run the pipeline to load data into BigQuery")
        return pd.DataFrame()

@st.cache_data
def load_sample_data():
    """Load sample restaurant data for demonstration when no real data is available."""
    np.random.seed(42)
    
    # Generate sample data
    n_restaurants = 50  # Reduced to make it clear it's demo data
    
    data = {
        'restaurant_id': [f'DEMO_{i:03d}' for i in range(1, n_restaurants + 1)],
        'restaurant_name': [f'Demo Restaurant {i}' for i in range(1, n_restaurants + 1)],
        'rating': np.random.normal(3.8, 0.8, n_restaurants).clip(1, 5),
        'rating_category': np.random.choice(['Excellent', 'Very Good', 'Good', 'Average', 'Below Average'], n_restaurants, p=[0.1, 0.2, 0.4, 0.2, 0.1]),
        'data_source': np.random.choice(['demo_data'], n_restaurants),  # Clear it's demo data
        'city': np.random.choice(['San Francisco', 'New York', 'Los Angeles'], n_restaurants),
        'state': np.random.choice(['CA', 'NY', 'CA'], n_restaurants),
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

# Load data - try real data first, fallback to demo data
df = load_real_data()
is_demo_data = df.empty

if is_demo_data:
    df = load_sample_data()
    
    # Add warning about demo data
    st.warning("""
    ⚠️ **You're viewing DEMO DATA** ⚠️
    
    This dashboard is showing sample data because:
    - Could not connect to BigQuery or no data found
    - To load real data, run: `python3 orchestration/flow.py --locations "San Francisco, CA"`
    - Check your BigQuery credentials in `.env` file
    - Verify your BigQuery dataset `restaurant_db` contains data
    
    **All restaurant names like "Demo Restaurant 1" are fake for demonstration purposes.**
    """)
else:
    st.success("✅ Showing real restaurant data from your pipeline!")

# Filters
st.sidebar.subheader("📍 Location Filter")

# Get available locations from configuration
try:
    available_locations = get_enabled_locations()
    if available_locations:
        selected_locations = st.sidebar.multiselect(
            "Select Locations",
            options=available_locations,
            default=available_locations,
            help="Choose which locations to include in the analysis"
        )
    else:
        selected_locations = []
        st.sidebar.warning("No locations configured. Please add locations using the location manager.")
except Exception as e:
    st.sidebar.error(f"Error loading locations: {e}")
    selected_locations = []

# Fallback to city-based filtering if no locations configured
if not selected_locations:
    selected_cities = st.sidebar.multiselect(
        "Select Cities (Fallback)",
        options=df['city'].unique(),
        default=list(df['city'].unique())
    )
else:
    # Filter by cities available in the data
    selected_cities = st.sidebar.multiselect(
        "Select Cities",
        options=df['city'].unique(),
        default=list(df['city'].unique())
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
        value=f"⭐ {avg_rating:.2f}",
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
    if not filtered_df.empty:
        highest_rating = filtered_df['rating'].max()
        st.metric(
            label="Highest Rating",
            value=f"⭐ {highest_rating:.1f}",
            delta=f"{highest_rating - df['rating'].max():.1f}"
        )
    else:
        st.metric(label="Highest Rating", value="N/A")

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
    price_rating = filtered_df.groupby('price_category')['rating'].mean().reset_index()
    fig_price_rating = px.bar(
        price_rating,
        x='price_category',
        y='rating',
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

# Rating Analysis Section
st.markdown("---")
st.subheader("⭐ Rating Analysis")

col1, col2 = st.columns(2)

with col1:
    # Rating distribution histogram
    fig_rating_dist = px.histogram(
        filtered_df,
        x='rating',
        nbins=20,
        title='Rating Distribution',
        labels={'rating': 'Rating', 'count': 'Number of Restaurants'},
        color_discrete_sequence=['#ff7f0e']
    )
    fig_rating_dist.update_layout(showlegend=False)
    st.plotly_chart(fig_rating_dist, use_container_width=True)

with col2:
    # Top rated restaurants
    top_rated = filtered_df.nlargest(10, 'rating')[['name', 'rating', 'city', 'review_count']]
    st.write("**🏆 Top 10 Highest Rated Restaurants**")
    
    for idx, row in top_rated.iterrows():
        stars = "⭐" * int(row['rating']) + ("⭐" if (row['rating'] - int(row['rating'])) >= 0.5 else "")
        st.write(f"**{row['name']}** - {stars} ({row['rating']:.1f})")
        st.caption(f"{row['city']} • {row['review_count']} reviews")
        st.write("")

# Data table
st.markdown("---")
st.subheader("📋 Detailed Data View")

# Add search functionality
search_term = st.text_input("🔍 Search restaurants by name:")

if search_term:
    search_df = filtered_df[filtered_df['name'].str.contains(search_term, case=False, na=False)]
else:
    search_df = filtered_df

# Display the data with enhanced ratings
display_df = search_df[['name', 'city', 'rating', 'price_category', 'review_count', 'business_tier', 'data_source']].copy()

# Add star rating column
def rating_to_stars(rating):
    """Convert numeric rating to star display."""
    if pd.isna(rating):
        return "No rating"
    
    full_stars = int(rating)
    half_star = 1 if (rating - full_stars) >= 0.5 else 0
    empty_stars = 5 - full_stars - half_star
    
    stars = "⭐" * full_stars + "⭐" * half_star + "☆" * empty_stars
    return f"{stars} ({rating:.1f})"

display_df['star_rating'] = display_df['rating'].apply(rating_to_stars)

# Reorder columns to show star ratings prominently
column_order = ['name', 'star_rating', 'city', 'price_category', 'review_count', 'business_tier', 'data_source']
st.dataframe(
    display_df[column_order],
    use_container_width=True,
    column_config={
        "name": st.column_config.TextColumn("Restaurant Name", width="medium"),
        "star_rating": st.column_config.TextColumn("Rating", width="medium"),
        "city": st.column_config.TextColumn("City", width="small"),
        "price_category": st.column_config.TextColumn("Price", width="small"),
        "review_count": st.column_config.NumberColumn("Reviews", width="small"),
        "business_tier": st.column_config.TextColumn("Tier", width="medium"),
        "data_source": st.column_config.TextColumn("Source", width="small")
    }
)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 1rem;'>
        🍽️ Restaurant Analytics Dashboard | Built with Streamlit, dbt, and BigQuery
    </div>
    """,
    unsafe_allow_html=True
)
