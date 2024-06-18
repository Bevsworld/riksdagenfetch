import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Table, Column, String, MetaData
from sqlalchemy.orm import sessionmaker
import dateparser
import time
import json

# Database credentials and connection setup
DATABASE_URL = "postgresql://retool:jr1cAFW3ZIwH@ep-tight-limit-a6uyk8mk.us-west-2.retooldb.com/retool?sslmode=require"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.bind = engine

# Define or load your table structure with new columns for download and speakerlist
riksdagen_table = Table('riksdagen', metadata,
                        Column('title', String),
                        Column('type', String),
                        Column('date', String),
                        Column('length', String),
                        Column('link', String),
                        Column('download', String),  # New column for downloadable link
                        Column('speakerlist', String),  # New column for speaker list as JSON
                        autoload_with=engine)

def check_and_insert_data():
    Session = sessionmaker(bind=engine)
    session = Session()

    response = requests.get('https://www.riksdagen.se/sv/sok/?avd=webbtv&doktyp=bet%2Cip')
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        content_ul = soup.select_one('#content > ul')
        for li in content_ul.find_all('li'):
            a_tag = li.find('a')
            aria_label = a_tag.get('aria-label')
            href = a_tag['href']
            full_link = href if href.startswith('http') else f"https://www.riksdagen.se{href}"
            if aria_label:
                parts = aria_label.split(',')
                event_type = parts[0].strip()
                title = parts[1].strip()
                date_string = parts[2].strip()
                duration = parts[3].strip()

                date_obj = dateparser.parse(date_string, languages=['sv'])
                formatted_date = date_obj.strftime('%Y-%m-%d') if date_obj else None

                exists = session.query(riksdagen_table).filter_by(link=full_link).first()
                if not exists:
                    try:
                        # Visit the page and scrape additional details
                        event_response = requests.get(full_link)
                        event_soup = BeautifulSoup(event_response.text, 'html.parser')
                        download_link_element = event_soup.select_one('#below-player > ul > li:nth-child(2) > a')
                        if download_link_element:
                            download_link = download_link_element['href']
                        else:
                            print("Download link not found for:", title)
                            download_link = None

                        speakers_list = event_soup.select_one('#speakers-list > ol')
                        speakers_data = {}
                        if speakers_list:
                            for speaker_item in speakers_list.find_all('li'):
                                speaker_name = speaker_item.select_one('a > span.sc-31b8789-2.fuVqcV').text
                                speaker_time = speaker_item.select_one('a > time').text
                                speakers_data[speaker_time] = speaker_name

                        new_record = riksdagen_table.insert().values(
                            title=title,
                            type=event_type,
                            date=formatted_date,
                            length=duration,
                            link=full_link,
                            download=download_link,
                            speakerlist=json.dumps(speakers_data)
                        )
                        session.execute(new_record)
                        session.commit()
                        print("New record inserted:", title)
                    except Exception as e:
                        print(f"An error occurred while processing {title}: {e}")
                else:
                    print("Record already exists:", title)
    session.close()

while True:
    print("Checking for new content...")
    check_and_insert_data()
    print("Waiting for an hour...")
    time.sleep(3600)  # Pause the script for an hour
