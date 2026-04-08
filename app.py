import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.express as px
import plotly.graph_objects as go
from fpdf import FPDF
import io
import gspread
from google.oauth2.service_account import Credentials
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

CRYPTO_ETFS = ["IBIT", "ETHA", "BSOL"]

st.set_page_config(page_title="Portfolio Manager OS", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

# Initialize session state for Google Sheets
if 'google_sheet_url' not in st.session_state:
    st.session_state.google_sheet_url = ""
if 'remember_sheet' not in st.session_state:
    st.session_state.remember_sheet = False
if 'google_credentials' not in st.session_state:
    st.session_state.google_credentials = None

# Local credentials file path
CREDENTIALS_FILE = ".streamlit/local_credentials.json"
CONFIG_FILE = ".streamlit/auto_sync_config.json"


# Load saved credentials on startup
def load_saved_credentials():
    """Load saved Google credentials from local file"""
    if os.path.exists(CREDENTIALS_FILE):
        try:
            with open(CREDENTIALS_FILE, 'r') as f:
                credentials_dict = json.load(f)
                st.session_state.google_credentials = credentials_dict
                return credentials_dict
        except Exception as e:
            st.warning(f"Could not load saved credentials: {e}")
    return None


def save_credentials(credentials_dict):
    """Save Google credentials to local file"""
    try:
        os.makedirs(os.path.dirname(CREDENTIALS_FILE), exist_ok=True)
        with open(CREDENTIALS_FILE, 'w') as f:
            json.dump(credentials_dict, f, indent=2)
        st.session_state.google_credentials = credentials_dict
        return True
    except Exception as e:
        st.error(f"Could not save credentials: {e}")
        return False


def load_auto_sync_config():
    """Load auto-sync config (credentials and URL) from local file"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                st.session_state.google_credentials = config.get('credentials')
                st.session_state.google_sheet_url = config.get('url', '')
                st.session_state.remember_sheet = True
                return True
        except Exception as e:
            st.warning(f"Could not load auto-sync config: {e}")
    return False


def save_auto_sync_config(credentials_dict, url):
    """Save auto-sync config to local file"""
    try:
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        config = {'credentials': credentials_dict, 'url': url}
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        st.error(f"Could not save auto-sync config: {e}")
        return False


# Load saved credentials on app startup
saved_creds = load_saved_credentials()

# Load auto-sync config on startup
load_auto_sync_config()


# Function to calculate additional metrics
def calculate_metrics(df):
    if 'Current_Value_ILS' in df.columns and 'Cost_ILS' in df.columns:
        total_cost = df['Cost_ILS'].sum()
        total_value = df['Current_Value_ILS'].sum()
        total_return = (total_value - total_cost) / total_cost if total_cost > 0 else 0
    else:
        total_return = 0

    # Placeholders for metrics that require historical daily data
    sharpe = 0.0
    volatility = 0.0

    return sharpe, volatility, total_return


def load_portfolio_data(csv_path):
    # Assume CSV has columns: Platform, Type, Ticker, Purchase_Date, Quantity, Origin_Buy_Price, Cost_Origin, Origin_Currency
    df = pd.read_csv(csv_path, parse_dates=['Purchase_Date'])
    return df


@st.cache_data(ttl=300)
def fetch_live_prices(unique_tickers, crypto_tickers):
    prices = {}

    def fetch_ticker(ticker):
        if ticker in crypto_tickers:
            yf_ticker = ticker + '-USD'
        else:
            yf_ticker = ticker
        try:
            t = yf.Ticker(yf_ticker)
            return ticker, t.info.get('regularMarketPrice', 0)
        except Exception as e:
            return ticker, 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_ticker, ticker) for ticker in unique_tickers]
        for future in as_completed(futures):
            ticker, price = future.result()
            prices[ticker] = price

    try:
        usdils_t = yf.Ticker('USDILS=X')
        usdils = usdils_t.info.get('regularMarketPrice', 0)
    except Exception as e:
        usdils = 0
    return prices, usdils


def process_data(df):
    numeric_cols = ['Quantity', 'Origin_Buy_Price', 'Cost_Origin', 'Cost_ILS', 'Current_Value_ILS', 'Cost_USD',
                    'Current_Value_USD', 'Buy_Price_USD', 'Buy_Price_ILS', 'Current_Price_USD', 'Current_Price_ILS',
                    'Return_Origin', 'Return_ILS', 'Commission']

    for col in numeric_cols:
        if col in df.columns:
            # Safely remove commas, ₪, $, %, and spaces
            df[col] = df[col].astype(str).str.replace(r'[₪$,\s%]', '', regex=True)
            # Convert to float and fill empty cells with 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Strip whitespaces from Status column and fill missing values with 'פתוח'
    if 'Status' in df.columns:
        df['Status'] = df['Status'].astype(str).str.strip()
        df['Status'] = df['Status'].fillna('פתוח')
        # Replace 'nan' string values (from coercion) with 'פתוח'
        df['Status'] = df['Status'].replace('nan', 'פתוח')

    # Parse dates safely
    if 'Purchase_Date' in df.columns:
        df['Purchase_Date'] = pd.to_datetime(df['Purchase_Date'], errors='coerce')

    return df


def create_summary(df):
    if 'Ticker' not in df.columns:
        return pd.DataFrame()

    agg_kwargs = {}
    if 'Quantity' in df.columns:
        agg_kwargs['Total_Qty'] = ('Quantity', 'sum')
    if 'Cost_ILS' in df.columns:
        agg_kwargs['Total_Cost_ILS'] = ('Cost_ILS', 'sum')
    if 'Current_Value_ILS' in df.columns:
        agg_kwargs['Total_Value_ILS'] = ('Current_Value_ILS', 'sum')
    if 'Cost_Origin' in df.columns:
        agg_kwargs['Total_Cost_Origin'] = ('Cost_Origin', 'sum')
    if 'Current_Value_USD' in df.columns:
        agg_kwargs['Total_Val_Origin'] = ('Current_Value_USD', 'sum')

    if not agg_kwargs:
        return pd.DataFrame()

    summary = df.groupby('Ticker').agg(**agg_kwargs).reset_index()

    if 'Total_Value_ILS' in summary.columns and 'Total_Cost_ILS' in summary.columns:
        summary['Profit_Loss_ILS'] = summary['Total_Value_ILS'] - summary['Total_Cost_ILS']
        # Safe division to prevent inf%
        summary['Avg_Return_ILS'] = summary.apply(
            lambda row: row['Profit_Loss_ILS'] / row['Total_Cost_ILS'] if row['Total_Cost_ILS'] > 0 else 0,
            axis=1
        )

    return summary


def sync_google_sheet(gc, sheet_url):
    """
    Sync data from a Google Sheet.

    Args:
        gc (gspread.Client): Authenticated gspread client.
        sheet_url (str): URL of the Google Sheet.

    Returns:
        pd.DataFrame: Data from the Google Sheet, or None if sync failed.
    """
    try:
        # Extract sheet key from URL
        # Example URL: https://docs.google.com/spreadsheets/d/1ABC123/edit#gid=0
        sheet_key = sheet_url.split('/d/')[1].split('/')[0]

        # Open the sheet and get the worksheet named "תמונת מצב"
        sheet = gc.open_by_key(sheet_key)
        worksheet = sheet.worksheet("תמונת מצב")

        # Get all values and convert to DataFrame
        data = worksheet.get_all_values()
        if not data:
            return None

        # First row is header, rest is data
        df = pd.DataFrame(data[1:], columns=data[0])
        # AGGRESSIVE column cleanup to strip invisible unicode characters (RTL/LTR marks, etc.)
        df.columns = [str(col).replace('\u200e', '').replace('\u200f', '').strip() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"❌ Sync Error: {repr(e)}")
        return None


# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Dashboard", "Analytics", "Reports", "Settings"])

# Data loading (common for all pages)
# Check if auto-sync is configured
auto_sync_configured = st.session_state.google_sheet_url != ""

df = None
sync_error = None

if auto_sync_configured:
    # Try to use saved credentials first
    credentials_dict = st.session_state.google_credentials

    if credentials_dict:
        try:
            # Authenticate using saved credentials
            credentials = Credentials.from_service_account_info(
                credentials_dict,
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            gc = gspread.authorize(credentials)

            # Load from Google Sheets
            df = sync_google_sheet(gc, st.session_state.google_sheet_url)
        except Exception as e:
            error_str = str(repr(e))
            # Distinguish between network errors and auth errors
            if "NameResolutionError" in error_str or "ConnectionError" in error_str or "Max retries" in error_str:
                sync_error = f"Network connectivity error: Unable to reach Google servers. Check your internet connection."
            else:
                sync_error = f"Authentication/Connection Error: {repr(e)}"
            df = None
    else:
        # Fallback: Manual upload
        uploaded_key = st.file_uploader("Upload Google Service Account Key (JSON) for auto-sync", type="json",
                                        key="gcp_key_main")

        if uploaded_key:
            try:
                # Authenticate and load from Google Sheets
                credentials_dict = json.load(uploaded_key)
                credentials = Credentials.from_service_account_info(
                    credentials_dict,
                    scopes=["https://www.googleapis.com/auth/spreadsheets"]
                )
                gc = gspread.authorize(credentials)

                # Load from Google Sheets
                df = sync_google_sheet(gc, st.session_state.google_sheet_url)
            except Exception as e:
                error_str = str(repr(e))
                if "NameResolutionError" in error_str or "ConnectionError" in error_str or "Max retries" in error_str:
                    sync_error = f"Network connectivity error: Unable to reach Google servers. Check your internet connection."
                else:
                    sync_error = f"Authentication/Connection Error: {repr(e)}"
                df = None

# If no data loaded yet, use sample data or show error
if df is None:
    if auto_sync_configured and sync_error:
        # Show error but allow fallback to manual upload
        st.warning(f"⚠️ Auto-sync failed: {sync_error}")
        st.info("💡 **Tip:** You can still upload a CSV file manually below, or check your internet connection and refresh the page.")
        st.divider()
        st.subheader("📁 Manual Portfolio Upload")
        uploaded_file = st.file_uploader("Upload Portfolio CSV", type="csv", key="csv_upload_fallback")
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            # AGGRESSIVE column cleanup to strip invisible unicode characters (RTL/LTR marks, etc.)
            df.columns = [str(col).replace('\u200e', '').replace('\u200f', '').strip() for col in df.columns]
            st.success("✅ CSV file loaded successfully!")
    elif not auto_sync_configured:
        # Manual mode with no upload - show upload UI
        uploaded_file = st.file_uploader("Upload Portfolio CSV", type="csv", key="csv_upload")
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            # AGGRESSIVE column cleanup to strip invisible unicode characters (RTL/LTR marks, etc.)
            df.columns = [str(col).replace('\u200e', '').replace('\u200f', '').strip() for col in df.columns]

# If still no data, use sample data
if df is None:
    # Use sample data
    sample_data = {
        'Platform': ['Bit2C', 'אקסלנס'],
        'Type': ['קריפטו', 'שוק ההון'],
        'Ticker': ['BTC', 'AAPL'],
        'Purchase_Date': ['2023-01-01', '2023-01-02'],
        'Quantity': [0.01, 10],
        'Origin_Buy_Price': [30000, 150],
        'Cost_Origin': [300, 1500],
        'Origin_Currency': ['USD', 'USD']
    }
    df = pd.DataFrame(sample_data)
    df['Purchase_Date'] = pd.to_datetime(df['Purchase_Date'])
    if not auto_sync_configured:
        st.info("📊 Showing sample data. Upload a CSV file to see your actual portfolio.")
else:
    # Process loaded data
    rename_dict = {
        "תאריך רכישה": "Purchase_Date",
        "פלטפורמה": "Platform",
        "נכס": "Ticker",
        "טיקר": "Ticker",
        "סוג": "Type",
        "סוג נכס": "Type",
        "כמות": "Quantity",
        "שער קנייה (מקור)": "Origin_Buy_Price",
        "שער קנייה": "Origin_Buy_Price",
        "עלות (₪)": "Cost_Origin",
        "עלות": "Cost_Origin",
        "עלות כוללת": "Cost_Origin",
        "מטבע": "Origin_Currency",
        "עלות (ILS)": "Cost_ILS",
        "שווי (ILS)": "Current_Value_ILS",
        "שווי עדכני (₪)": "Current_Value_ILS",
        "שווי ILS": "Current_Value_ILS",
        "תשואה מקור": "Return_Origin",
        "תשואה שקלית": "Return_ILS",
        "עלות (USD)": "Cost_USD",
        "שווי (USD)": "Current_Value_USD",
        "שווי נוכחי USD": "Current_Value_USD",
        "שער קנייה (USD)": "Buy_Price_USD",
        "שער קנייה (ILS)": "Buy_Price_ILS",
        "שער נוכחי (USD)": "Current_Price_USD",
        "שער נוכחי (ILS)": "Current_Price_ILS",
        "עמלה": "Commission",
        "סטטוס": "Status",
    }
    df.rename(columns=rename_dict, inplace=True)
    required_columns = ['Purchase_Date', 'Platform', 'Ticker', 'Type', 'Quantity', 'Origin_Buy_Price', 'Cost_Origin']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        st.error(
            f"Required columns not found after renaming: {missing_columns}. Please check the CSV headers and update the rename_dict.")
        st.stop()
    if 'Origin_Currency' not in df.columns:
        df['Origin_Currency'] = 'ILS'  # Assume costs are in ILS if not specified
    # Clean numeric columns by removing commas and converting to float
    numeric_cols = ['Quantity', 'Origin_Buy_Price', 'Cost_Origin']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
    df['Purchase_Date'] = pd.to_datetime(df['Purchase_Date'], format='%d/%m/%Y')

processed_df = process_data(df)

# Initialize Status column if it doesn't exist
if 'Status' not in processed_df.columns:
    processed_df['Status'] = 'פתוח'

summary = create_summary(processed_df)

# Safe global metric calculation
if not summary.empty and 'Total_Value_ILS' in summary.columns and 'Total_Cost_ILS' in summary.columns:
    total_value_ils = summary['Total_Value_ILS'].sum()
    total_cost_ils = summary['Total_Cost_ILS'].sum()
    total_yield = (total_value_ils - total_cost_ils) / total_cost_ils if total_cost_ils > 0 else 0
else:
    total_value_ils = 0
    total_cost_ils = 0
    total_yield = 0

if page == "Dashboard":
    st.title("Portfolio Manager OS - Dashboard")

    # Split into open and closed positions
    open_positions = processed_df[processed_df['Status'] == 'פתוח'].copy() if 'Status' in processed_df.columns else processed_df.copy()
    closed_positions = processed_df[processed_df['Status'] == 'סגור'].copy() if 'Status' in processed_df.columns else pd.DataFrame()

    # Generate summary from open positions only
    summary_open = create_summary(open_positions)

    # Calculate metrics for open positions
    if not summary_open.empty and 'Total_Value_ILS' in summary_open.columns and 'Total_Cost_ILS' in summary_open.columns:
        total_value_ils_open = summary_open['Total_Value_ILS'].sum()
        total_cost_ils_open = summary_open['Total_Cost_ILS'].sum()
        total_yield_open = (total_value_ils_open - total_cost_ils_open) / total_cost_ils_open if total_cost_ils_open > 0 else 0
    else:
        total_value_ils_open = 0
        total_cost_ils_open = 0
        total_yield_open = 0

    # Calculate realized profit from closed positions
    if not closed_positions.empty and 'Current_Value_ILS' in closed_positions.columns and 'Cost_ILS' in closed_positions.columns:
        realized_profit = closed_positions['Current_Value_ILS'].sum() - closed_positions['Cost_ILS'].sum()
    else:
        realized_profit = 0

    # Modern KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        profit_loss_open = total_value_ils_open - total_cost_ils_open
        st.metric("Total Value (ILS)", f"₪{total_value_ils_open:,.0f}",
                  delta=f"₪{profit_loss_open:,.0f}" if profit_loss_open != 0 else None)
    with col2:
        st.metric("Total Cost (ILS)", f"₪{total_cost_ils_open:,.0f}")
    with col3:
        st.metric("Total Net Yield (%)", f"{total_yield_open:.2%}", delta=f"{total_yield_open:.2%}")
    with col4:
        st.metric("Realized Profit (רווח ממומש)", f"₪{realized_profit:,.0f}",
                  delta=f"₪{realized_profit:,.0f}" if realized_profit != 0 else None)

    # Visuals - using open positions summary
    if not summary_open.empty and 'Total_Value_ILS' in summary_open.columns and 'Profit_Loss_ILS' in summary_open.columns:
        col1, col2 = st.columns(2)
        with col1:
            # Donut Chart for Allocation
            fig_donut = px.pie(summary_open, values='Total_Value_ILS', names='Ticker', hole=0.4,
                               title='Portfolio Allocation (Open Positions)')
            fig_donut.update_layout(margin=dict(t=40, b=40, l=40, r=40))
            st.plotly_chart(fig_donut)
        with col2:
            # Bar Chart for Profit/Loss
            fig_bar = px.bar(summary_open, x='Ticker', y='Profit_Loss_ILS', title='Profit/Loss by Ticker (Open)',
                             color='Profit_Loss_ILS', color_continuous_scale='RdYlGn')
            fig_bar.update_layout(margin=dict(t=40, b=40, l=40, r=40))
            st.plotly_chart(fig_bar)

    # Summary table
    st.subheader("Summary by Ticker (Open Positions)")
    # Dynamically select columns that exist in the summary
    available_cols = ['Ticker']
    for col in ['Total_Qty', 'Total_Cost_ILS', 'Total_Value_ILS', 'Profit_Loss_ILS', 'Avg_Return_ILS']:
        if col in summary_open.columns:
            available_cols.append(col)

    if len(available_cols) > 1:  # More than just 'Ticker'
        format_dict = {
            'Total_Qty': '{:.8f}',
            'Total_Cost_ILS': '₪{:,.0f}',
            'Total_Value_ILS': '₪{:,.0f}',
            'Profit_Loss_ILS': '₪{:,.0f}',
            'Avg_Return_ILS': '{:.2%}'
        }
        # Filter format_dict to only include columns that exist
        format_dict = {k: v for k, v in format_dict.items() if k in summary_open.columns}
        formatted_summary = summary_open[available_cols].style.format(format_dict).hide(axis='index')
        st.dataframe(formatted_summary)

    # Drill-down
    st.subheader("Drill-Down View (Open Positions)")
    ticker_options = ['Select All'] + sorted(summary_open['Ticker'].tolist()) if not summary_open.empty else ['Select All']
    selected_ticker = st.selectbox("Select Ticker", ticker_options)
    desired_columns = ['Purchase_Date', 'Platform', 'Quantity', 'Origin_Buy_Price', 'Cost_ILS', 'Current_Value_ILS', 'Commission', 'Status']
    available_columns = [col for col in desired_columns if col in open_positions.columns]
    if selected_ticker == 'Select All':
        drill_df = open_positions[available_columns]
    else:
        drill_df = open_positions[open_positions['Ticker'] == selected_ticker][available_columns]
    formatted_drill = drill_df.style.format({
        'Quantity': '{:.8f}',
        'Origin_Buy_Price': '{:.2f}',
        'Cost_ILS': '₪{:,.0f}',
        'Current_Value_ILS': '₪{:,.0f}',
        'Commission': '₪{:,.0f}'
    }).hide(axis='index')
    st.dataframe(formatted_drill)

    # Closed Positions Section
    if not closed_positions.empty:
        st.divider()
        st.subheader("Closed Positions (סגור)")
        summary_closed = create_summary(closed_positions)

        if not summary_closed.empty:
            available_cols_closed = ['Ticker']
            for col in ['Total_Qty', 'Total_Cost_ILS', 'Total_Value_ILS', 'Profit_Loss_ILS', 'Avg_Return_ILS']:
                if col in summary_closed.columns:
                    available_cols_closed.append(col)

            if len(available_cols_closed) > 1:
                format_dict_closed = {
                    'Total_Qty': '{:.8f}',
                    'Total_Cost_ILS': '₪{:,.0f}',
                    'Total_Value_ILS': '₪{:,.0f}',
                    'Profit_Loss_ILS': '₪{:,.0f}',
                    'Avg_Return_ILS': '{:.2%}'
                }
                format_dict_closed = {k: v for k, v in format_dict_closed.items() if k in summary_closed.columns}
                formatted_summary_closed = summary_closed[available_cols_closed].style.format(format_dict_closed).hide(axis='index')
                st.dataframe(formatted_summary_closed)

elif page == "Analytics":
    st.title("Portfolio Manager OS - Analytics")
    if not open_positions.empty:
        sharpe, vol, tot_ret = calculate_metrics(open_positions)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Sharpe Ratio", f"{sharpe:.2f}")
        with col2:
            st.metric("Volatility", f"{vol:.2%}")
        with col3:
            st.metric("Total Return", f"{tot_ret:.2%}")

        # Historical chart
        plot_df = open_positions.dropna(subset=['Purchase_Date']).sort_values('Purchase_Date')
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=plot_df['Purchase_Date'], y=plot_df['Current_Value_ILS'], mode='lines+markers',
                                 name='Portfolio Value'))
        fig.update_layout(title='Portfolio Value Over Time (Open Positions)', xaxis_title='Date', yaxis_title='Value (ILS)')
        st.plotly_chart(fig)

elif page == "Reports":
    st.title("Portfolio Manager OS - Reports")
    # Reports
    report_options = [
        "Select Report",
        "Crypto Allocation (%)",
        "Asset Concentration",
        "Biggest Winner & Loser",
        "Net Investment (Deposits)",
        "Live Exchange Rates"
    ]
    selected_report = st.selectbox("Select Report", report_options)
    if selected_report == "Crypto Allocation (%)":
        crypto_df = open_positions[open_positions['Type'].isin(['קריפטו']) | open_positions['Ticker'].isin(CRYPTO_ETFS)]
        crypto_val = crypto_df['Current_Value_ILS'].sum()
        btc_df = open_positions[open_positions['Ticker'].isin(['BTC', 'IBIT'])]
        btc_val = btc_df['Current_Value_ILS'].sum()
        p_crypto = crypto_val / total_value_ils_open if total_value_ils_open else 0
        p_btc_total = btc_val / total_value_ils_open if total_value_ils_open else 0
        p_btc_crypto = btc_val / crypto_val if crypto_val else 0
        st.write(f"Percentage of Crypto in total portfolio: {p_crypto:.2%}")
        st.write(f"Percentage of BTC in total portfolio: {p_btc_total:.2%}")
        st.write(f"Percentage of BTC out of total Crypto: {p_btc_crypto:.2%}")
    elif selected_report == "Asset Concentration":
        assets = ['BTC', 'ETH', 'SOL']
        etfs = {'BTC': 'IBIT', 'ETH': 'ETHA', 'SOL': 'BSOL'}
        data = []
        for asset in assets:
            real_df = open_positions[(open_positions['Ticker'] == asset) & (open_positions['Type'] == 'קריפטו')]
            real_qty = real_df['Quantity'].sum()
            real_val = real_df['Current_Value_ILS'].sum()
            etf_df = open_positions[open_positions['Ticker'] == etfs[asset]]
            etf_qty = etf_df['Quantity'].sum()
            etf_val = etf_df['Current_Value_ILS'].sum()
            total_exp = real_val + etf_val
            data.append([asset, real_qty, real_val, etf_qty, etf_val, total_exp])
        conc_df = pd.DataFrame(data, columns=['Asset', 'Direct Qty', 'Direct Val (ILS)', 'ETF Qty', 'ETF Val (ILS)',
                                              'Total Exposure (ILS)'])
        st.dataframe(conc_df.style.format({
            'Direct Qty': '{:.8f}',
            'Direct Val (ILS)': '₪{:,.0f}',
            'ETF Qty': '{:.8f}',
            'ETF Val (ILS)': '₪{:,.0f}',
            'Total Exposure (ILS)': '₪{:,.0f}'
        }))
    elif selected_report == "Biggest Winner & Loser":
        if 'Total_Value_ILS' in summary_open.columns and 'Total_Cost_ILS' in summary_open.columns:
            summary_open['Dynamic_Return'] = (summary_open['Total_Value_ILS'] - summary_open['Total_Cost_ILS']) / summary_open[
                'Total_Cost_ILS']
            winner = summary_open.loc[summary_open['Dynamic_Return'].idxmax()]
            loser = summary_open.loc[summary_open['Dynamic_Return'].idxmin()]
            st.write(f"Biggest Winner: {winner['Ticker']} with {winner['Dynamic_Return']:.2%}")
            st.write(f"Biggest Loser: {loser['Ticker']} with {loser['Dynamic_Return']:.2%}")
        else:
            st.write("Insufficient data to calculate winners and losers.")
    elif selected_report == "Net Investment (Deposits)":
        platform_summary = open_positions.groupby('Platform').agg(
            Total_Cost_ILS=('Cost_ILS', 'sum'),
            Total_Value_ILS=('Current_Value_ILS', 'sum')
        ).reset_index()
        platform_summary['Profit_Loss_ILS'] = platform_summary['Total_Value_ILS'] - platform_summary['Total_Cost_ILS']
        platform_summary['Profit_Loss_Percent'] = platform_summary['Profit_Loss_ILS'] / platform_summary[
            'Total_Cost_ILS']
        total_row = pd.DataFrame({
            'Platform': ['Total'],
            'Total_Cost_ILS': [platform_summary['Total_Cost_ILS'].sum()],
            'Total_Value_ILS': [platform_summary['Total_Value_ILS'].sum()],
            'Profit_Loss_ILS': [platform_summary['Profit_Loss_ILS'].sum()],
            'Profit_Loss_Percent': [
                platform_summary['Profit_Loss_ILS'].sum() / platform_summary['Total_Cost_ILS'].sum()]
        })
        platform_summary = pd.concat([platform_summary, total_row], ignore_index=True)
        st.dataframe(platform_summary.style.format({
            'Total_Cost_ILS': '₪{:,.0f}',
            'Total_Value_ILS': '₪{:,.0f}',
            'Profit_Loss_ILS': '₪{:,.0f}',
            'Profit_Loss_Percent': '{:.2%}'
        }))
    elif selected_report == "Live Exchange Rates":
        try:
            usdils_live = yf.Ticker('USDILS=X').info['regularMarketPrice']
        except:
            usdils_live = 0
        try:
            btcusd = yf.Ticker('BTC-USD').info['regularMarketPrice']
        except:
            btcusd = 0
        try:
            ethusd = yf.Ticker('ETH-USD').info['regularMarketPrice']
        except:
            ethusd = 0
        try:
            solusd = yf.Ticker('SOL-USD').info['regularMarketPrice']
        except:
            solusd = 0
        st.write(f"USD/ILS: {usdils_live:.4f}")
        st.write(f"BTC/USD: {btcusd:.2f}")
        st.write(f"ETH/USD: {ethusd:.2f}")
        st.write(f"SOL/USD: {solusd:.2f}")

    # Export to PDF
    if st.button("Export to PDF"):
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        pdf.cell(200, 10, txt="Portfolio Report", ln=True, align='C')
        # Add data
        pdf.cell(200, 10, txt=f"Total Value: ₪{total_value_ils:,.0f}", ln=True)
        pdf.cell(200, 10, txt=f"Total Cost: ₪{total_cost_ils:,.0f}", ln=True)
        pdf.cell(200, 10, txt=f"Total Yield: {total_yield:.2%}", ln=True)
        # Save to bytes
        pdf_output = io.BytesIO()
        pdf.output(pdf_output)
        pdf_output.seek(0)
        st.download_button(label="Download PDF", data=pdf_output, file_name="portfolio_report.pdf",
                           mime="application/pdf")

elif page == "Settings":
    st.title("Portfolio Manager OS - Settings")
    st.subheader("🔗 Google Sheets Integration - Auto-Sync Setup")

    st.write("**One-time setup**: Configure automatic syncing from Google Sheets")
    st.write("---")

    # Instructions
    with st.expander("📋 How to Set Up Google Credentials", expanded=False):
        st.write("""
        1. Go to [Google Cloud Console](https://console.cloud.google.com)
        2. Create a new project
        3. Enable "Google Sheets API"
        4. Create a Service Account:
           - Go to "Service Accounts"
           - Create new service account
           - Grant "Editor" role
           - Create JSON key and download
        5. Share your Google Sheet with the service account email
        """)

    st.write("---")

    # Upload Google credentials
    st.subheader("Step 1: Upload Google Service Account Key")
    uploaded_key = st.file_uploader("Upload Google Service Account Key (JSON)", type="json", key="gcp_key")

    if uploaded_key:
        # Save credentials to local file
        credentials_dict = json.load(uploaded_key)
        if save_credentials(credentials_dict):
            st.success("✅ Google credentials saved successfully!")

    st.write("---")

    # Google Sheet URL input with session state
    st.subheader("Step 2: Enter Google Sheet URL")

    col1, col2 = st.columns([3, 1])
    with col1:
        sheet_url = st.text_input(
            "Google Sheet URL",
            value=st.session_state.google_sheet_url,
            placeholder="https://docs.google.com/spreadsheets/d/1ABC123/edit"
        )

    with col2:
        remember = st.checkbox("Remember URL", value=st.session_state.remember_sheet)

    st.write("---")

    # Setup auto-sync
    if st.button("✅ Set Up Auto-Sync", key="setup_sync"):
        if uploaded_key and sheet_url:
            # Save settings to session state
            st.session_state.google_sheet_url = sheet_url
            st.session_state.remember_sheet = remember

            # Also save to config file
            save_auto_sync_config(st.session_state.google_credentials, sheet_url)

            st.success("""
            ✅ **Setup Complete!**

            **Next Steps:**
            1. **Refresh the app** (F5) to start auto-syncing
            2. The app will automatically load your portfolio from Google Sheets
            3. No need to upload CSV files anymore
            4. Updates to your Google Sheet will appear automatically

            **Note:** If you see any errors, try refreshing again or check that your Google Sheet is properly shared with the service account.
            """)

            st.info("💡 **Credentials are saved locally** - you won't need to upload the JSON file again tomorrow!")
        else:
            st.error("⚠️ Please provide both the Google credentials file and Sheet URL")

    # Show current settings
    if st.session_state.google_sheet_url:
        st.write("---")
        st.subheader("📊 Current Configuration")
        st.write(f"**Sheet URL**: `{st.session_state.google_sheet_url}`")
        st.write(f"**Remember Settings**: {st.session_state.remember_sheet}")

        if st.button("🔄 Clear Settings"):
            st.session_state.google_sheet_url = ""
            st.session_state.remember_sheet = False
            st.rerun()

    st.write("---")
    st.subheader("🔄 Sync Status")
    if auto_sync_configured:
        if df is not None:
            st.success(f"✅ Synced {len(df)} rows from Google Sheet!")
        else:
            st.error("❌ Failed to sync from Google Sheet.")
            if sync_error:
                st.info(f"Error details: {sync_error}")
    else:
        st.info("Auto-sync not configured.")

    st.write("---")
    st.subheader("🔧 Troubleshooting")
    with st.expander("Connection Issues & Fixes", expanded=False):
        st.write("""
        **DNS Resolution Error** (Failed to resolve 'oauth2.googleapis.com')
        - ✅ Check your internet connection
        - ✅ Try refreshing the page (F5)
        - ✅ If using VPN/Proxy, verify it allows access to Google services
        - ✅ Try clearing browser cache (Ctrl+Shift+Del)
        - ✅ If issue persists, use manual CSV upload as a fallback
        
        **Authentication Error** (Invalid credentials)
        - ✅ Download a fresh Google Service Account JSON key
        - ✅ Re-upload the key in Step 1 above
        - ✅ Verify the service account email has access to your Google Sheet
        - ✅ Share the Google Sheet with the service account email explicitly
        
        **Sheet Not Found**
        - ✅ Verify the Google Sheet URL is correct
        - ✅ Check that the sheet has a worksheet named "תמונת מצב"
        - ✅ Ensure the service account has access to the sheet
        """)

    with st.expander("Test Connection", expanded=False):
        if st.button("🧪 Test Google Sheets Connection"):
            if st.session_state.google_credentials:
                try:
                    test_credentials = Credentials.from_service_account_info(
                        st.session_state.google_credentials,
                        scopes=["https://www.googleapis.com/auth/spreadsheets"]
                    )
                    test_gc = gspread.authorize(test_credentials)
                    st.success("✅ Google authentication successful!")
                    
                    if st.session_state.google_sheet_url:
                        try:
                            sheet_key = st.session_state.google_sheet_url.split('/d/')[1].split('/')[0]
                            test_sheet = test_gc.open_by_key(sheet_key)
                            st.success(f"✅ Google Sheet accessible! Found {len(test_sheet.worksheets())} worksheets")
                            
                            # List worksheets
                            worksheet_names = [ws.title for ws in test_sheet.worksheets()]
                            st.info(f"Available worksheets: {', '.join(worksheet_names)}")
                            
                            if "תמונת מצב" in worksheet_names:
                                st.success("✅ 'תמונת מצב' worksheet found!")
                            else:
                                st.warning("⚠️ 'תמונת מצב' worksheet not found. Update the sheet name if different.")
                        except Exception as e:
                            st.error(f"❌ Could not access Google Sheet: {str(e)}")
                except Exception as e:
                    error_str = str(e)
                    if "NameResolutionError" in error_str or "ConnectionError" in error_str:
                        st.error("❌ Network Error: Cannot reach Google servers. Check your internet connection.")
                    else:
                        st.error(f"❌ Authentication failed: {str(e)}")
            else:
                st.warning("⚠️ No Google credentials saved. Please upload credentials first.")


def authenticate_google_sheets():
    """
    Authenticate with Google Sheets using secrets or manual upload.

    Returns:
        gspread.Client: Authenticated gspread client, or None if authentication failed.
    """
    try:
        # Check if credentials exist in st.secrets
        if "gcp_service_account" in st.secrets:
            st.info("✅ Using Google credentials from Streamlit Secrets")
            credentials = Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            gc = gspread.authorize(credentials)
            return gc
    except Exception as e:
        st.warning(f"⚠️ Could not load credentials from secrets: {e}")

    # Fallback: Manual file upload
    st.write("Google credentials not found in secrets. Uploading manually...")

    uploaded_key = st.file_uploader("Upload Google Service Account Key (JSON)", type="json")

    if uploaded_key is not None:
        try:
            credentials_dict = json.load(uploaded_key)
            credentials = Credentials.from_service_account_info(
                credentials_dict,
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            gc = gspread.authorize(credentials)
            st.success("✅ Successfully authenticated with Google Sheets!")
            return gc
        except Exception as e:
            st.error(f"❌ Failed to authenticate: {e}")
            return None

    return None

