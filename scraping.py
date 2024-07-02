# Script: Billboard Scraping Official Code
import requests
import pandas as pd
import datetime
import numpy as np
from time import sleep
from bs4 import BeautifulSoup

# Function to get the weekly chart data
def bb_get_weekly_chart(chart_route, chart_date):
    # Specify the target Hot 100 chart page URL.
    billboard_url = 'https://www.billboard-japan.com/charts/detail'
    valid_chart_routes = ['hot100']  # Valid chart routes
    retry_max = 4  # Maximum retry attempts for connection

    if chart_route in valid_chart_routes:
        # Split the date into year, month, day
        year, month, day = chart_date.split('-')
        # Construct the request URL
        request_url = f"{billboard_url}?a={chart_route}&year={year}&month={month}&day={day}"
        # Attempt to connect and fetch data
        chart_request_data = _attempt_connections(request_url, retry_max)
        
        if chart_request_data.status_code == requests.codes.ok:
            soup = BeautifulSoup(chart_request_data.content, 'html.parser')
            # Extract chart data
            chart_data = _bb_extract_chart_data(soup, chart_date)
            return chart_data
        else:
            print(f"Connection error on {request_url}, {retry_max + 1} times attempted.")
            return pd.DataFrame(columns=['ranking', 'artist', 'title', 
                                         'last_week_rank', 'peak_position', 
                                         'weeks_on_chart', 'chart_date'])
        
    else:
        print(f"{chart_route} is not a valid Billboard chart route")
        return pd.DataFrame(columns=['ranking', 'artist', 'title', 
                                     'last_week_rank', 'peak_position', 
                                     'weeks_on_chart', 'chart_date'])

# Function to get multiple charts data within a date range
def bb_get_multiple_charts(chart_route, chart_from_date, chart_to_date):
    # Get initial weekly chart data
    chart_data_collection_df = bb_get_weekly_chart(chart_route, chart_from_date)
    chart_date_index = datetime.datetime.strptime(chart_from_date, "%Y-%m-%d") + datetime.timedelta(days=7)
    chart_to_datetime = datetime.datetime.strptime(chart_to_date, "%Y-%m-%d")
    # Loop through the date range and collect weekly data
    while chart_date_index <= chart_to_datetime:
        chart_data_collection_df = pd.concat([chart_data_collection_df, 
                                              bb_get_weekly_chart(chart_route, chart_date_index.strftime('%Y-%m-%d'))])
        chart_date_index = chart_date_index + datetime.timedelta(days=7)
        sleep(1)  # Sleep for 1 second between requests to avoid overloading the server
    print("Finished gathering data from Billboard.com, formatting data...")
    # Format collected data
    chart_data_collection_df = _bb_format_columns(chart_data_collection_df)
    return chart_data_collection_df

# Helper function to attempt connections with retry logic
def _attempt_connections(conn_url, retries):
    chart_request_result = requests.get(conn_url)
    success_bool = chart_request_result.status_code == requests.codes.ok
    retry_num = 1
    while not success_bool and retry_num <= retries:
        print(f"{conn_url} retry number {retry_num}")
        chart_request_result = requests.get(conn_url)
        success_bool = chart_request_result.status_code == requests.codes.ok
        retry_num += 1
    return chart_request_result

# Helper function to format the columns of the chart data
def _bb_format_columns(chart_df):
    formatting_chart = chart_df
    # Replace invalid characters and clean up the data
    formatting_chart.last_week_rank = formatting_chart.last_week_rank.map(lambda lw: str(lw).replace('-', '0').strip())
    formatting_chart.title = formatting_chart.title.map(lambda t: t.replace('&amp;', '&').replace('&#039;', "'").replace('&quot;', '"').strip())
    formatting_chart.artist = formatting_chart.artist.map(lambda a: a.replace('&amp;', '&').replace('&#039;', "'").replace('&quot;', '"').strip())
    formatting_chart.ranking = formatting_chart.ranking.map(lambda r: str(r).strip())
    formatting_chart.peak_position = formatting_chart.peak_position.map(lambda p: str(p).strip())
    formatting_chart.weeks_on_chart = formatting_chart.weeks_on_chart.map(lambda wc: str(wc).strip())
    formatting_chart.artist = np.where(formatting_chart['artist'] == '', '===<Missing_Artist>===', formatting_chart.artist)
    return formatting_chart

# Helper function to extract chart data from the HTML content
def _bb_extract_chart_data(soup, chart_date):
    chart_parsed_data = pd.DataFrame(columns=['ranking', 'artist', 'title', 
                                              'last_week_rank', 'peak_position', 
                                              'weeks_on_chart', 'chart_date'])
    # Select the rows containing the chart data
    chart_items = soup.select('tr[class^="rank"]')
    for item in chart_items:
        try:
            ranking_elem = item.select_one('.rank_detail')
            ranking = ranking_elem.text.strip() if ranking_elem else 'N/A'
            
            title_elem = item.select_one('.name_detail')
            title = title_elem.text.strip() if title_elem else 'N/A'
            
            artist_elem = item.select_one('.artist_name')
            artist = artist_elem.text.strip() if artist_elem else 'N/A'
            
            last_week_elem = item.select_one('.rank_detail_sp_obj')
            last_week_rank = last_week_elem.text.strip() if last_week_elem else 'N/A'
            
            peak_position = ranking
            weeks_on_chart = '1'  # Default value as the weeks on chart is not provided in the HTML structure shown

            # Create a DataFrame entry for the current chart item
            chart_entry_data = pd.DataFrame({
                'ranking': ranking, 
                'artist': artist, 
                'title': title, 
                'last_week_rank': last_week_rank, 
                'peak_position': peak_position, 
                'weeks_on_chart': weeks_on_chart, 
                'chart_date': chart_date
            }, index=[str(chart_date) + "_" + ranking.strip()])

            # Append the entry to the parsed data DataFrame
            chart_parsed_data = pd.concat([chart_parsed_data, chart_entry_data])
        except Exception as e:
            print(f"Error parsing chart data: {e}")
            continue
    
    return chart_parsed_data

# Specify the date range for collecting weekly chart data
# Change 'chart_from_date' and 'chart_to_date' to the desired date range
# Note: Ensure to start from a date that actually exists on the Billboard site.  
# Confirm the starting month and the available dates using the GUI before running the script. See an actual web page of your start month. 
chart_data = bb_get_multiple_charts('hot100', '2019-01-07', '2019-01-31')

# Save the collected data to a CSV file
print(chart_data)
chart_data.to_csv('test.csv', index=False)
