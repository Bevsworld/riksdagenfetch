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
from tenacity import retry, wait_exponential, stop_after_attempt
import sys
import smtplib
from email.mime.text import MIMEText

# Configure logging to both console and file
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')

# File handler
file_handler = logging.FileHandler('riksdagen_scraper.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

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
    hours = 0
    minutes = 0
    seconds = 0

    hour_match = re.search(r'(\d+) timmar', duration_str)
    minute_match = re.search(r'(\d+) minuter', duration_str)
    second_match = re.search(r'(\d+) sekunder', duration_str)

    if hour_match:
        hours = int(hour_match.group(1))
    if minute_match:
        minutes = int(minute_match.group(1))
    if second_match:
        seconds = int(second_match.group(1))

    total_seconds = hours * 3600 + minutes * 60 + seconds
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


@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def get_db_session():
    try:
        Session = sessionmaker(bind=engine)
        return Session()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise


def send_email(subject, message):
    sender = "your_email@example.com"
    recipients = ["admin@example.com"]
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    try:
        server = smtplib.SMTP('smtp.example.com')
        server.login("your_username", "your_password")
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logging.info("Error notification sent.")
    except Exception as e:
        logging.error(f"Failed to send email notification: {e}")


def check_and_insert_data():
    session = get_db_session()

    try:
        response = requests.get('https://www.riksdagen.se/sv/sok/?avd=webbtv&doktyp=bet%2Cip')
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        return

    if response.status_code == 200:
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            content_ul = soup.select_one('#content > ul')
            for li in content_ul.find_all('li'):
                try:
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

                        if duration_in_seconds > 600:
                            exists = session.query(riksdagen_table).filter_by(link=full_link).first()
                            if not exists:
                                try:
                                    unique_name = generate_unique_name()
                                    create_folder_in_space(unique_name)

                                    event_response = requests.get(full_link)
                                    event_response.raise_for_status()
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
                                        spacesfolder=unique_name,
                                        edited=False,
                                        uploadedtospaces=False
                                    )
                                    session.execute(new_record)
                                    session.commit()
                                    logging.info(f"New record inserted: {title}")
                                except Exception as e:
                                    logging.error(f"An error occurred while processing {title}: {e}")
                                    session.rollback()
                            else:
                                logging.info(f"Record already exists: {title}")
                        else:
                            logging.info(f"Skipping video shorter than 10 minutes: {title}")
                except Exception as e:
                    logging.error(f"Error processing list item: {e}")
        except Exception as e:
            logging.error(f"Error parsing HTML content: {e}")
    else:
        logging.error(f"Failed to fetch data: status code {response.status_code}")

    session.close()


def main():
    try:
        while True:
            logging.info("Checking for new content...")
            check_and_insert_data()
            logging.info("Waiting for an hour....")
            time.sleep(3600)  # Pause the script for an hour
    except Exception as e:
        logging.critical(f"Unexpected error: {e}")
        send_email("Riksdagen Scraper Critical Error", f"Unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
