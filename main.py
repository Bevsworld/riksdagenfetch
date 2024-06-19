import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Table, Column, String, MetaData, Integer, Boolean
from sqlalchemy.orm import sessionmaker
import dateparser
import time
import json
import re
import logging
import boto3
import random
import string
import threading
from moviepy.video.io.ffmpeg_tools import ffmpeg_extract_subclip
from tenacity import retry, wait_exponential, stop_after_attempt

# Configure logging
logging.basicConfig(filename='riksdagen_scraper.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# DigitalOcean Spaces configuration
DO_SPACES_ACCESS_KEY = 'DO009U4RBZ8UJAVE8DPL'
DO_SPACES_SECRET_KEY = 'NEh7GbCufcqpWqtFc91qTsGtJAaV6nnGD8qaLkVm5kU'
DO_SPACES_ENDPOINT = 'https://fra1.digitaloceanspaces.com'
DO_SPACES_BUCKET = 'samladpolitik'

# Initialize DigitalOcean Spaces client
session = boto3.session.Session()
client = session.client('s3',
                        region_name='fra1',
                        endpoint_url=DO_SPACES_ENDPOINT,
                        aws_access_key_id=DO_SPACES_ACCESS_KEY,
                        aws_secret_access_key=DO_SPACES_SECRET_KEY)

# Database credentials and connection setup
DATABASE_URL = "postgresql://retool:jr1cAFW3ZIwH@ep-tight-limit-a6uyk8mk.us-west-2.retooldb.com/retool?sslmode=require"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.bind = engine

# Define or load your table structure with new columns for download, speakerlist, spacesfolder, edited, and uploadedtospaces
riksdagen_table = Table('riksdagen', metadata,
                        Column('title', String),
                        Column('type', String),
                        Column('date', String),
                        Column('length', Integer),  # Updated to Integer for storing seconds
                        Column('link', String),
                        Column('download', String),  # New column for downloadable link
                        Column('speakerlist', String),  # New column for speaker list as JSON
                        Column('spacesfolder', String),  # New column for DigitalOcean Spaces folder name
                        Column('edited', Boolean, default=False),  # New column for edited status
                        Column('uploadedtospaces', Boolean, default=False),  # New column for upload status
                        autoload_with=engine)

def convert_duration_to_seconds(duration_str):
    minutes = 0
    seconds = 0
    minute_match = re.search(r'(\d+) minuter', duration_str)
    second_match = re.search(r'(\d+) sekunder', duration_str)

    if minute_match:
        minutes = int(minute_match.group(1))
    if second_match:
        seconds = int(second_match.group(1))

    total_seconds = minutes * 60 + seconds
    return total_seconds

def generate_unique_name(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def create_folder_in_space(folder_name):
    try:
        # Create a new folder in the DigitalOcean Space
        client.put_object(Bucket=DO_SPACES_BUCKET, Key=f"{folder_name}/", Body='')
        logging.info(f"Folder created in DigitalOcean Space: {folder_name}/")
    except Exception as e:
        logging.error(f"Failed to create folder in DigitalOcean Space: {folder_name}/, Error: {e}")
        raise

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
def get_db_session():
    Session = sessionmaker(bind=engine)
    return Session()

def check_and_insert_data():
    session = get_db_session()

    try:
        response = requests.get('https://www.riksdagen.se/sv/sok/?avd=webbtv&doktyp=bet%2Cip')
        response.raise_for_status()  # Raise an error for bad status codes
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        return

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

                duration_in_seconds = convert_duration_to_seconds(duration)

                # Only proceed if the duration is longer than 600 seconds (10 minutes)
                if duration_in_seconds > 600:
                    exists = session.query(riksdagen_table).filter_by(link=full_link).first()
                    if not exists:
                        try:
                            # Generate a unique name for the new entry
                            unique_name = generate_unique_name()

                            # Create a new folder in the DigitalOcean Space
                            create_folder_in_space(unique_name)

                            # Visit the page and scrape additional details
                            event_response = requests.get(full_link)
                            event_response.raise_for_status()  # Raise an error for bad status codes
                            event_soup = BeautifulSoup(event_response.text, 'html.parser')
                            download_link_element = event_soup.select_one('#below-player > ul > li:nth-child(2) > a')
                            if download_link_element:
                                download_link = download_link_element['href']
                            else:
                                logging.warning(f"Download link not found for: {title}")
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
                                length=duration_in_seconds,
                                link=full_link,
                                download=download_link,
                                speakerlist=json.dumps(speakers_data),
                                spacesfolder=unique_name,  # Add the unique name to the database entry as folder name
                                edited=False,  # Initialize edited status as False
                                uploadedtospaces=False  # Initialize upload status as False
                            )
                            session.execute(new_record)
                            session.commit()
                            logging.info(f"New record inserted: {title}")
                        except Exception as e:
                            logging.error(f"An error occurred while processing {title}: {e}")
                    else:
                        logging.info(f"Record already exists: {title}")
                else:
                    logging.info(f"Skipping video shorter than 10 minutes: {title}")
    else:
        logging.error(f"Failed to fetch data: status code {response.status_code}")

    session.close()

def process_videos():
    while True:
        logging.info("Checking for entries to process...")
        session = get_db_session()

        try:
            entries = session.query(riksdagen_table).filter_by(edited=False).all()
            for entry in entries:
                download_link = entry.download
                video_id = entry.spacesfolder
                speakerlist = json.loads(entry.speakerlist) if isinstance(entry.speakerlist, str) else entry.speakerlist
                logging.info(f"Processing video {video_id}...")

                # Step 2: Download the video
                video_path = f"/tmp/{video_id}.mp4"
                logging.info(f"Downloading video from {download_link}")
                response = requests.get(download_link)
                with open(video_path, 'wb') as f:
                    f.write(response.content)
                logging.info(f"Downloaded video {video_id} to {video_path}")

                # Step 3: Cut the video into smaller clips
                for timestamp, speaker in speakerlist.items():
                    logging.info(f"Processing clip for {speaker} starting at {timestamp}")
                    start_time = sum(x * int(t) for x, t in zip([60, 1], timestamp.split(":")))
                    end_time = start_time + 30  # Assuming each clip is 30 seconds long for this example
                    clip_path = f"/tmp/{video_id}_{speaker}.mp4"
                    logging.info(f"Cutting clip for {speaker} from {start_time} to {end_time}")
                    ffmpeg_extract_subclip(video_path, start_time, end_time, targetname=clip_path)
                    logging.info(f"Cut clip for {speaker} to {clip_path}")

                    # Step 5: Store each video file inside the DigitalOcean Spaces storage
                    try:
                        logging.info(f"Uploading clip for {speaker} to DigitalOcean Spaces: {video_id}/{speaker}.mp4")
                        client.upload_file(clip_path, DO_SPACES_BUCKET, f"{video_id}/{speaker}.mp4")
                        logging.info(f"Uploaded clip for {speaker} to {video_id}/{speaker}.mp4")
                    except Exception as e:
                        logging.error(f"Failed to upload clip for {speaker}: {e}")

                # Step 6: Update the database entry
                entry.edited = True
                entry.uploadedtospaces = True
                session.commit()
                logging.info(f"Updated database entry for {video_id}")
        except Exception as e:
            logging.error(f"An error occurred during video processing: {e}")
        finally:
            session.close()

        logging.info("Waiting for next check...")
        time.sleep(3600)  # Wait for an hour before checking again

def main():
    video_thread = threading.Thread(target=process_videos)
    video_thread.start()

    while True:
        logging.info("Checking for new content...")
        check_and_insert_data()
        logging.info("Waiting for an hour....")
        time.sleep(3600)  # Pause the script for an hour

if __name__ == "__main__":
    main()
