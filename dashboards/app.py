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

# Helper function for consistent chart theming
def apply_dark_theme(fig):
    """Apply consistent dark theme to plotly charts."""
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='#e6edf3',
        title_font_color='#f0f6fc',
        showlegend=False,
        title_font_size=16,
        title_x=0.5
    )
    fig.update_xaxes(gridcolor='#21262d', linecolor='#30363d')
    fig.update_yaxes(gridcolor='#21262d', linecolor='#30363d')
    return fig

# Page configuration
st.set_page_config(
    page_title="Restaurant Analytics",
    page_icon="◉",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Obsidian-inspired dark theme CSS
st.markdown("""
<style>
    .stApp {
        background-color: #0d1117;
        color: #e6edf3;
    }
    
    .main-header {
        font-size: 2.5rem;
        color: #e6edf3;
        font-weight: 300;
        text-align: center;
        margin-bottom: 3rem;
        border-bottom: 1px solid #21262d;
        padding-bottom: 1rem;
    }
    
    .metric-container {
        background-color: #161b22;
        border: 1px solid #21262d;
        border-radius: 6px;
        padding: 1.5rem;
        margin: 0.5rem 0;
    }
    
    .sidebar .sidebar-content {
        background-color: #0d1117;
        border-right: 1px solid #21262d;
    }
    
    .stSelectbox > div > div {
        background-color: #21262d;
        color: #e6edf3;
        border: 1px solid #30363d;
    }
    
    .stMultiSelect > div > div {
        background-color: #21262d;
        color: #e6edf3;
        border: 1px solid #30363d;
    }
    
    .stSlider > div > div > div {
        background-color: #21262d;
    }
    
    .section-header {
        color: #f0f6fc;
        font-size: 1.2rem;
        font-weight: 400;
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #21262d;
    }
    
    .data-card {
        background-color: #161b22;
        border: 1px solid #21262d;
        border-radius: 6px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    
    .stDataFrame {
        background-color: #161b22;
        border: 1px solid #21262d;
        border-radius: 6px;
    }
    
    .stDataFrame > div {
        background-color: #161b22;
    }
    
    .stAlert {
        background-color: #161b22;
        border: 1px solid #f85149;
        color: #e6edf3;
        border-radius: 6px;
    }
    
    .stSuccess {
        background-color: #161b22;
        border: 1px solid #3fb950;
        color: #e6edf3;
        border-radius: 6px;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">Restaurant Analytics</h1>', unsafe_allow_html=True)

# Sidebar for filters
st.sidebar.markdown("### Filters")

# Location Management Section
st.sidebar.markdown("#### Locations")
if st.sidebar.button("View All Locations"):
    try:
        locations = get_enabled_locations()
        if locations:
            st.sidebar.success(f"{len(locations)} locations configured")
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
        
        # Query the dbt mart instead of raw data (all transformations handled by dbt)
        query = f"""
        SELECT 
            restaurant_id,
            restaurant_name as name,
            rating,
            rating_category,
            review_count,
            review_volume_category,
            price_level,
            price_category,
            business_tier,
            market_positioning,
            city,
            state,
            full_location,
            formatted_address,
            latitude,
            longitude,
            data_source,
            data_quality_score,
            weighted_rating_score,
            categories,
            ingestion_timestamp,
            processed_at,
            dbt_updated_at
        FROM `{config.get('project_id')}.{config.get('dataset_id')}.dim_restaurants`
        WHERE data_quality_score >= 75  -- Filter for quality data
        ORDER BY weighted_rating_score DESC, review_count DESC
        LIMIT 500
        """
        
        # Execute query and convert to DataFrame  
        df = client.query(query).to_dataframe()
        
        if not df.empty:
            
            st.success(f"Loaded {len(df)} restaurants from BigQuery")
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error loading data from BigQuery: {e}")
        st.info("Make sure you've run the pipeline to load data into BigQuery")
        return pd.DataFrame()

@st.cache_data
def load_sample_data():
    """Load sample restaurant data for demonstration when no real data is available."""
    np.random.seed(42)
    
    # Generate sample data
    n_restaurants = 50  # Reduced to make it clear it's demo data
    
    data = {
        'restaurant_id': [f'DEMO_{i:03d}' for i in range(1, n_restaurants + 1)],
        'name': [f'Demo Restaurant {i}' for i in range(1, n_restaurants + 1)],  # Changed from restaurant_name to name
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
    **Demo Mode**
    
    This dashboard is showing sample data because:
    - Could not connect to BigQuery or no data found
    - To load real data, run: `python3 orchestration/flow.py --locations "San Francisco, CA"`
    - Check your BigQuery credentials in `.env` file
    - Verify your BigQuery dataset `restaurant_db` contains data
    
    All restaurant names like "Demo Restaurant 1" are fake for demonstration purposes.
    """)
else:
    st.success("Showing real restaurant data from your pipeline")

# Filters
st.sidebar.markdown("#### Location Filter")

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

st.sidebar.markdown("#### Rating Filter")
min_rating = st.sidebar.slider("Minimum Rating", 1.0, 5.0, 3.0, 0.1)

st.sidebar.markdown("#### Price Filter")
selected_price_levels = st.sidebar.multiselect(
    "Select Price Levels",
    options=df['price_category'].unique(),
    default=df['price_category'].unique()
)

st.sidebar.markdown("#### Data Source")
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
    if not filtered_df.empty:
        highest_rating = filtered_df['rating'].max()
        st.metric(
            label="Highest Rating",
            value=f"{highest_rating:.1f}",
            delta=f"{highest_rating - df['rating'].max():.1f}"
        )
    else:
        st.metric(label="Highest Rating", value="N/A")

# Charts
st.markdown("---")
st.markdown('<div class="section-header">Analytics Overview</div>', unsafe_allow_html=True)

# Create two columns for charts
col1, col2 = st.columns(2)

with col1:
    # Rating distribution
    fig_rating = px.histogram(
        filtered_df,
        x='rating',
        nbins=20,
        title='Rating Distribution',
        color_discrete_sequence=['#58a6ff']
    )
    fig_rating = apply_dark_theme(fig_rating)
    st.plotly_chart(fig_rating, use_container_width=True)

with col2:
    # Rating by city
    city_ratings = filtered_df.groupby('city')['rating'].mean().sort_values(ascending=False)
    fig_city = px.bar(
        x=city_ratings.index,
        y=city_ratings.values,
        title='Average Rating by City',
        color_discrete_sequence=['#7c3aed']
    )
    fig_city = apply_dark_theme(fig_city)
    st.plotly_chart(fig_city, use_container_width=True)

# Price level analysis
st.markdown('<div class="section-header">Price Level Analysis</div>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    # Price level distribution
    price_counts = filtered_df['price_category'].value_counts()
    fig_price = px.pie(
        values=price_counts.values,
        names=price_counts.index,
        title='Restaurant Distribution by Price Level',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig_price = apply_dark_theme(fig_price)
    st.plotly_chart(fig_price, use_container_width=True)

with col2:
    # Rating vs Price Level
    price_rating = filtered_df.groupby('price_category')['rating'].mean().reset_index()
    fig_price_rating = px.bar(
        price_rating,
        x='price_category',
        y='rating',
        title='Average Rating by Price Level',
        color_discrete_sequence=['#22c55e']
    )
    fig_price_rating = apply_dark_theme(fig_price_rating)
    st.plotly_chart(fig_price_rating, use_container_width=True)

# New dbt-powered analytics sections
st.markdown('<div class="section-header">Market Intelligence</div>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    # Market positioning analysis
    if 'market_positioning' in filtered_df.columns:
        positioning_counts = filtered_df['market_positioning'].value_counts()
        fig_market = px.pie(
            values=positioning_counts.values,
            names=positioning_counts.index,
            title='Market Positioning Distribution',
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_market = apply_dark_theme(fig_market)
        st.plotly_chart(fig_market, use_container_width=True)

with col2:
    # Business tier distribution
    tier_counts = filtered_df['business_tier'].value_counts()
    fig_tier = px.bar(
        x=tier_counts.index,
        y=tier_counts.values,
        title='Business Tier Distribution',
        color_discrete_sequence=['#f59e0b']
    )
    fig_tier = apply_dark_theme(fig_tier)
    st.plotly_chart(fig_tier, use_container_width=True)

with col3:
    # Data quality overview
    if 'data_quality_score' in filtered_df.columns:
        # Create quality tiers based on score
        def quality_tier(score):
            if score >= 90: return 'Excellent'
            elif score >= 75: return 'Good' 
            elif score >= 50: return 'Fair'
            else: return 'Poor'
        
        filtered_df['quality_tier'] = filtered_df['data_quality_score'].apply(quality_tier)
        quality_counts = filtered_df['quality_tier'].value_counts()
        fig_quality = px.pie(
            values=quality_counts.values,
            names=quality_counts.index,
            title='Data Quality Distribution',
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        fig_quality = apply_dark_theme(fig_quality)
        st.plotly_chart(fig_quality, use_container_width=True)

# Advanced analytics section
st.markdown('<div class="section-header">Advanced Analytics</div>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    # Weighted rating score vs review count scatter plot
    if 'weighted_rating_score' in filtered_df.columns:
        fig_scatter = px.scatter(
            filtered_df,
            x='review_count',
            y='weighted_rating_score',
            color='business_tier',
            size='data_quality_score' if 'data_quality_score' in filtered_df.columns else None,
            title='Weighted Rating Score vs Review Count',
            hover_data=['name', 'city', 'market_positioning'] if 'market_positioning' in filtered_df.columns else ['name', 'city']
        )
        fig_scatter = apply_dark_theme(fig_scatter)
        st.plotly_chart(fig_scatter, use_container_width=True)

with col2:
    # Business tier performance (using what we have instead of market_segment)
    if 'business_tier' in filtered_df.columns and 'weighted_rating_score' in filtered_df.columns:
        tier_performance = filtered_df.groupby('business_tier').agg({
            'rating': 'mean',
            'review_count': 'mean',
            'weighted_rating_score': 'mean'
        }).round(2)
        
        fig_tier_perf = px.bar(
            tier_performance,
            x=tier_performance.index,
            y='weighted_rating_score',
            title='Average Performance by Business Tier',
            color_discrete_sequence=['#8b5cf6']
        )
        fig_tier_perf = apply_dark_theme(fig_tier_perf)
        st.plotly_chart(fig_tier_perf, use_container_width=True)

# Data freshness and quality metrics
st.markdown('<div class="section-header">Data Quality Metrics</div>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

with col1:
    if 'data_quality_score' in filtered_df.columns:
        avg_quality = filtered_df['data_quality_score'].mean()
        st.metric("Average Data Quality", f"{avg_quality:.1f}/100")

with col2:
    # Calculate data freshness based on ingestion_timestamp
    if 'ingestion_timestamp' in filtered_df.columns:
        import pandas as pd
        from datetime import datetime, timedelta
        
        now = pd.Timestamp.now()
        recent_data = filtered_df['ingestion_timestamp'] > (now - timedelta(days=2))
        fresh_data_pct = (recent_data.sum() / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
        st.metric("Fresh Data %", f"{fresh_data_pct:.1f}%")

with col3:
    # Calculate valid coordinates percentage
    if 'latitude' in filtered_df.columns and 'longitude' in filtered_df.columns:
        valid_coords = (~filtered_df['latitude'].isna()) & (~filtered_df['longitude'].isna())
        valid_coords_pct = (valid_coords.sum() / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
        st.metric("Valid Coordinates %", f"{valid_coords_pct:.1f}%")

with col4:
    premium_tier_pct = (filtered_df['business_tier'] == 'Premium').sum() / len(filtered_df) * 100
    st.metric("Premium Tier %", f"{premium_tier_pct:.1f}%")

# Data source comparison
st.markdown('<div class="section-header">Data Source Comparison</div>', unsafe_allow_html=True)

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
st.markdown('<div class="section-header">Business Tier Analysis</div>', unsafe_allow_html=True)

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
st.markdown('<div class="section-header">Rating Analysis</div>', unsafe_allow_html=True)

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
    st.write("**Top 10 Highest Rated Restaurants**")
    
    for idx, row in top_rated.iterrows():
        st.write(f"**{row['name']}** - {row['rating']:.1f}")
        st.caption(f"{row['city']} • {row['review_count']} reviews")
        st.write("")

# Data table
st.markdown("---")
st.markdown('<div class="section-header">Detailed Data View</div>', unsafe_allow_html=True)

# Add search functionality
search_term = st.text_input("Search restaurants by name:")

if search_term:
    search_df = filtered_df[filtered_df['name'].str.contains(search_term, case=False, na=False)]
else:
    search_df = filtered_df

# Display the data with enhanced ratings
display_df = search_df[['name', 'city', 'rating', 'price_category', 'review_count', 'business_tier', 'data_source']].copy()

# Add formatted rating column
def format_rating(rating):
    """Convert numeric rating to clean display."""
    if pd.isna(rating):
        return "No rating"
    return f"{rating:.1f}/5.0"

display_df['formatted_rating'] = display_df['rating'].apply(format_rating)

# Reorder columns to show ratings prominently
column_order = ['name', 'formatted_rating', 'city', 'price_category', 'review_count', 'business_tier', 'data_source']
st.dataframe(
    display_df[column_order],
    use_container_width=True,
    column_config={
        "name": st.column_config.TextColumn("Restaurant Name", width="medium"),
        "formatted_rating": st.column_config.TextColumn("Rating", width="medium"),
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
    <div style='text-align: center; color: #7d8590; padding: 1rem; font-size: 0.9rem;'>
        Restaurant Analytics | Built with Streamlit, dbt, and BigQuery
    </div>
    """,
    unsafe_allow_html=True
)
