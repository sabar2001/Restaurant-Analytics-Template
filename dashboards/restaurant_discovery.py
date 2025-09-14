#!/usr/bin/env python3
"""
Restaurant Discovery Dashboard
Focus: Find restaurants by cuisine, explore city food scenes, practical insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import re
from datetime import datetime
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
        title_font_size=14,
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

def extract_cuisines(types_str):
    """Extract cuisine types from restaurant types string."""
    if pd.isna(types_str):
        return []
    
    # Clean up the types string and extract cuisine keywords
    types_clean = str(types_str).lower()
    
    cuisine_keywords = {
        'italian': ['italian', 'pizza', 'pasta'],
        'chinese': ['chinese', 'dim_sum'],
        'mexican': ['mexican', 'taco', 'burrito'],
        'japanese': ['japanese', 'sushi', 'ramen'],
        'indian': ['indian', 'curry'],
        'thai': ['thai'],
        'korean': ['korean', 'barbecue'],
        'american': ['american', 'burger', 'barbecue', 'steakhouse'],
        'french': ['french', 'bistro'],
        'mediterranean': ['mediterranean', 'greek'],
        'vietnamese': ['vietnamese', 'pho'],
        'seafood': ['seafood', 'fish'],
        'breakfast': ['breakfast', 'brunch', 'cafe'],
        'dessert': ['dessert', 'ice_cream', 'bakery'],
        'fast_food': ['fast_food', 'quick_service'],
        'fine_dining': ['fine_dining', 'upscale']
    }
    
    found_cuisines = []
    for cuisine, keywords in cuisine_keywords.items():
        if any(keyword in types_clean for keyword in keywords):
            found_cuisines.append(cuisine.title())
    
    return found_cuisines if found_cuisines else ['Other']

# Page configuration
st.set_page_config(
    page_title="Restaurant Discovery",
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
        font-size: 2.5rem;
        color: #58a6ff;
        font-weight: 600;
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .cuisine-tag {
        background: linear-gradient(135deg, #1a1f36, #2d3748);
        border: 1px solid #58a6ff;
        border-radius: 20px;
        padding: 0.3rem 0.8rem;
        margin: 0.2rem;
        display: inline-block;
        font-size: 0.8rem;
        color: #58a6ff;
    }
    
    .restaurant-card {
        background: linear-gradient(135deg, #161b22, #21262d);
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
    }
    
    .rating-excellent { color: #22c55e; }
    .rating-good { color: #3b82f6; }
    .rating-average { color: #f59e0b; }
    .rating-poor { color: #ef4444; }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">🍽️ Restaurant Discovery</h1>', unsafe_allow_html=True)

# Load real data from BigQuery
@st.cache_data
def load_restaurant_data():
    """Load restaurant data with cuisine extraction."""
    try:
        from google.cloud import bigquery
        from ingestion.utils import get_bigquery_config
        
        config = get_bigquery_config()
        if not config:
            return None, "BigQuery configuration missing"
        
        client = bigquery.Client(project=config.get('project_id'))
        
        query = f"""
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
            CASE 
              WHEN price_level = 1 THEN '$'
              WHEN price_level = 2 THEN '$$'
              WHEN price_level = 3 THEN '$$$'
              WHEN price_level = 4 THEN '$$$$'
              ELSE 'N/A'
            END as price_display,
            ingestion_timestamp
        FROM `{config.get('project_id')}.{config.get('dataset_id')}.raw_restaurant_data`
        WHERE rating IS NOT NULL
        ORDER BY rating DESC, user_ratings_total DESC
        """
        
        df = client.query(query).to_dataframe()
        
        # Extract cuisines
        df['cuisines'] = df['types'].apply(extract_cuisines)
        df['cuisine_list'] = df['cuisines'].apply(lambda x: ', '.join(x))
        
        return df, None
        
    except Exception as e:
        return None, str(e)

# Load data
df, error = load_restaurant_data()

if error:
    st.error(f"⚠️ Could not load data: {error}")
    st.stop()

if df is None or df.empty:
    st.warning("📊 No restaurant data found. Please run the data pipeline first.")
    st.stop()

# Sidebar filters
st.sidebar.markdown("### 🔍 Find Restaurants")

# City selection
cities = sorted(df['city'].dropna().unique())
selected_city = st.sidebar.selectbox(
    "📍 Choose a City",
    options=['All Cities'] + cities,
    help="Select a city to explore its food scene"
)

# Cuisine filter
all_cuisines = []
for cuisine_list in df['cuisines']:
    all_cuisines.extend(cuisine_list)
unique_cuisines = sorted(list(set(all_cuisines)))

selected_cuisines = st.sidebar.multiselect(
    "🍕 Cuisine Types", 
    options=unique_cuisines,
    default=[],
    help="Filter by specific cuisine types"
)

# Rating filter
min_rating = st.sidebar.slider(
    "⭐ Minimum Rating", 
    min_value=1.0, 
    max_value=5.0, 
    value=3.5, 
    step=0.1,
    help="Only show restaurants above this rating"
)

# Price filter
price_options = ['$', '$$', '$$$', '$$$$', 'N/A']
selected_prices = st.sidebar.multiselect(
    "💰 Price Range",
    options=price_options,
    default=price_options,
    help="Filter by price level"
)

# Review count filter
min_reviews = st.sidebar.slider(
    "📝 Minimum Reviews",
    min_value=0,
    max_value=int(df['review_count'].max()),
    value=10,
    help="Filter by number of reviews (popularity indicator)"
)

# Apply filters
filtered_df = df.copy()

if selected_city != 'All Cities':
    filtered_df = filtered_df[filtered_df['city'] == selected_city]

if selected_cuisines:
    filtered_df = filtered_df[filtered_df['cuisines'].apply(
        lambda x: any(cuisine in x for cuisine in selected_cuisines)
    )]

filtered_df = filtered_df[
    (filtered_df['rating'] >= min_rating) &
    (filtered_df['price_display'].isin(selected_prices)) &
    (filtered_df['review_count'] >= min_reviews)
]

# Main dashboard
st.markdown(f"### 📊 Found {len(filtered_df)} restaurants")

if len(filtered_df) == 0:
    st.warning("No restaurants match your criteria. Try adjusting the filters.")
    st.stop()

# Quick stats
col1, col2, col3, col4 = st.columns(4)

with col1:
    avg_rating = filtered_df['rating'].mean()
    st.metric("Average Rating", f"{avg_rating:.1f}⭐")

with col2:
    total_reviews = filtered_df['review_count'].sum()
    st.metric("Total Reviews", f"{total_reviews:,}")

with col3:
    price_mode = filtered_df['price_display'].mode()
    most_common_price = price_mode.iloc[0] if len(price_mode) > 0 else "N/A"
    st.metric("Most Common Price", most_common_price)

with col4:
    top_cuisine = pd.Series([c for cuisines in filtered_df['cuisines'] for c in cuisines]).value_counts()
    popular_cuisine = top_cuisine.index[0] if len(top_cuisine) > 0 else "Various"
    st.metric("Popular Cuisine", popular_cuisine)

# City Analysis (if single city selected)
if selected_city != 'All Cities':
    st.markdown(f"## 🏙️ {selected_city} Food Scene")
    
    city_data = df[df['city'] == selected_city]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Cuisine distribution in the city
        city_cuisines = []
        for cuisine_list in city_data['cuisines']:
            city_cuisines.extend(cuisine_list)
        
        cuisine_counts = pd.Series(city_cuisines).value_counts().head(10)
        
        fig_cuisine = px.bar(
            x=cuisine_counts.values,
            y=cuisine_counts.index,
            orientation='h',
            title=f'Popular Cuisines in {selected_city}',
            labels={'x': 'Number of Restaurants', 'y': 'Cuisine Type'}
        )
        fig_cuisine = apply_dark_theme(fig_cuisine)
        st.plotly_chart(fig_cuisine, width='stretch')
    
    with col2:
        # Price distribution
        price_dist = city_data['price_display'].value_counts()
        
        fig_price = px.pie(
            values=price_dist.values,
            names=price_dist.index,
            title=f'Price Distribution in {selected_city}'
        )
        fig_price = apply_dark_theme(fig_price)
        st.plotly_chart(fig_price, width='stretch')

# Restaurant listings
st.markdown("## 🍽️ Restaurant Listings")

# Sort options
sort_option = st.selectbox(
    "Sort by:",
    options=['Rating (High to Low)', 'Rating (Low to High)', 'Most Reviews', 'Alphabetical'],
    index=0
)

if sort_option == 'Rating (High to Low)':
    display_df = filtered_df.sort_values(['rating', 'review_count'], ascending=[False, False])
elif sort_option == 'Rating (Low to High)':
    display_df = filtered_df.sort_values(['rating', 'review_count'], ascending=[True, False])
elif sort_option == 'Most Reviews':
    display_df = filtered_df.sort_values('review_count', ascending=False)
else:  # Alphabetical
    display_df = filtered_df.sort_values('name')

# Display restaurants as cards
for idx, restaurant in display_df.head(20).iterrows():
    
    # Rating color coding
    rating_class = "rating-excellent" if restaurant['rating'] >= 4.5 else \
                   "rating-good" if restaurant['rating'] >= 4.0 else \
                   "rating-average" if restaurant['rating'] >= 3.5 else "rating-poor"
    
    # Create restaurant card
    st.markdown(f"""
    <div class="restaurant-card">
        <h4>{restaurant['name']}</h4>
        <p><span class="{rating_class}">⭐ {restaurant['rating']:.1f}</span> 
           ({restaurant['review_count']:,} reviews) • {restaurant['price_display']}</p>
        <p>📍 {restaurant['formatted_address']}</p>
        <div>
            {''.join([f'<span class="cuisine-tag">{cuisine}</span>' for cuisine in restaurant['cuisines']])}
        </div>
    </div>
    """, unsafe_allow_html=True)

if len(display_df) > 20:
    st.info(f"Showing top 20 results. Total {len(display_df)} restaurants match your criteria.")

# City comparison (if All Cities selected)
if selected_city == 'All Cities' and len(selected_cuisines) > 0:
    st.markdown("## 🗺️ City Comparison")
    
    cuisine_city_data = []
    for cuisine in selected_cuisines:
        city_stats = []
        for city in cities:
            city_cuisine_df = df[
                (df['city'] == city) & 
                (df['cuisines'].apply(lambda x: cuisine in x))
            ]
            if len(city_cuisine_df) > 0:
                city_stats.append({
                    'city': city,
                    'cuisine': cuisine,
                    'count': len(city_cuisine_df),
                    'avg_rating': city_cuisine_df['rating'].mean(),
                    'avg_reviews': city_cuisine_df['review_count'].mean()
                })
        cuisine_city_data.extend(city_stats)
    
    if cuisine_city_data:
        comparison_df = pd.DataFrame(cuisine_city_data)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_count = px.bar(
                comparison_df,
                x='city',
                y='count',
                color='cuisine',
                title='Restaurant Count by City and Cuisine',
                labels={'count': 'Number of Restaurants'}
            )
            fig_count = apply_dark_theme(fig_count)
            st.plotly_chart(fig_count, width='stretch')
        
        with col2:
            fig_rating = px.bar(
                comparison_df,
                x='city', 
                y='avg_rating',
                color='cuisine',
                title='Average Rating by City and Cuisine',
                labels={'avg_rating': 'Average Rating'}
            )
            fig_rating = apply_dark_theme(fig_rating)
            st.plotly_chart(fig_rating, width='stretch')

# Footer
st.markdown("---")
st.markdown("### 💡 Tips for Better Results")
st.markdown("""
- **Narrow your search**: Select a specific city and cuisine for targeted results
- **Adjust rating threshold**: Lower the minimum rating to see more options
- **Check review count**: More reviews often indicate established, popular places
- **Try different cuisines**: Explore the food diversity in each city
""")

st.markdown(
    """
    <div style='text-align: center; color: #7d8590; padding: 1rem; font-size: 0.9rem;'>
        🍽️ Restaurant Discovery Tool | Find your next great meal
    </div>
    """,
    unsafe_allow_html=True
)
