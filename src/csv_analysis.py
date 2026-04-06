import pandas as pd
import os

# GSC
gsc_path = r'c:\Users\yourh\Desktop\PainpointToday\Yourhome_AnalysisDB\data\yourhome.co.th-Performance-on-Search-2026-04-06\Queries.csv'
gsc_df = pd.read_csv(gsc_path)
print("--- GSC Queries ---")
print(gsc_df.head(10))

# GA Pages
ga_pages_path = r'c:\Users\yourh\Desktop\PainpointToday\Yourhome_AnalysisDB\data\Pages_and_screens_Page_path_and_screen_class.csv'
# Skip 9 rows of metadata
ga_pages_df = pd.read_csv(ga_pages_path, skiprows=9)
print("\n--- GA Top 15 Pages ---")
print(ga_pages_df[['Page path and screen class', 'Views', 'Active users', 'Average engagement time per active user']].head(15))

# GA Events
ga_events_path = r'c:\Users\yourh\Desktop\PainpointToday\Yourhome_AnalysisDB\data\Events_Event_name.csv'
ga_events_df = pd.read_csv(ga_events_path, skiprows=9)
print("\n--- GA Events ---")
print(ga_events_df[['Event name', 'Event count', 'Total users']])
