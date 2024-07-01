Document: Understanding the Flow and Process of the Riksdagen Scraper Script
Overview
This document explains the flow and functionality of a Python script designed to scrape video content from the Riksdagen website, process the data, and store it in a database. The script also handles cloud storage management, logging, and error notification.

Key Components
Imports and Libraries:

The script leverages various libraries such as requests, BeautifulSoup, sqlalchemy, dateparser, json, re, logging, boto3, random, string, tenacity, smtplib, and email.mime.text for web scraping, database interaction, date parsing, logging, AWS S3 client configuration, error handling, and email notifications.
Logging Configuration:

Configures logging to log messages to both a file (riksdagen_scraper.log) and the console.
DigitalOcean Spaces Configuration:

Sets up a DigitalOcean Spaces client for cloud storage using credentials and endpoint details.
Database Configuration:

Sets up the connection to a PostgreSQL database using SQLAlchemy and defines the table structure for riksdagen.
Main Functions and Their Flow
Function: convert_duration_to_seconds:

Converts a duration string (e.g., "1 hour 30 minutes") into total seconds.
Function: generate_unique_name:

Generates a unique name using random letters and digits for creating folders in DigitalOcean Spaces.
Function: create_folder_in_space:

Creates a new folder in the DigitalOcean Space for storing video files.
Function: get_db_session:

Creates a new database session using SQLAlchemy. Utilizes the tenacity library to retry in case of connection issues.
Function: send_email:

Sends an email notification for critical errors.
Function: check_and_insert_data:

The core function that:
Fetches the webpage content from the Riksdagen website.
Parses the HTML content to extract video metadata.
Converts the video duration to seconds.
Checks if the video already exists in the database.
If not, creates a new folder in DigitalOcean Spaces.
Fetches the download link for the video.
Extracts the speaker list and their respective times.
Inserts a new record into the database with the extracted data.
Function: main:

The main loop that continuously checks for new content every hour and processes it using the check_and_insert_data function.
Sends an email notification if a critical error occurs and exits the script.
Execution Flow
Initialization:

The script starts by configuring logging, setting up database connections, and initializing the DigitalOcean Spaces client.
Main Loop:

The main function initiates the process by calling check_and_insert_data.
It then pauses for an hour before rechecking for new content, ensuring continuous operation without manual intervention.
Processing Data:

check_and_insert_data fetches and parses the webpage content.
It processes each list item to extract relevant metadata, convert durations, check for existing records, create cloud storage folders, fetch download links, extract speaker lists, and insert new records into the database.
Error Handling and Retries:

The script uses the tenacity library to implement exponential backoff retries for database operations.
Logs errors at each step to facilitate debugging and maintenance.
Sends an email notification for critical errors that halt the script.
Scheduling:

The time.sleep(3600) call in the main loop ensures that the script checks for new content every hour.
Error Notifications
Function: send_email:
Sends an email notification to specified recipients when a critical error occurs, including the error message in the email body.
This detailed explanation should help understand the workflow and processes involved in the script, from initialization to web scraping, data processing, database interactions, cloud storage management, error handling, and scheduling mechanisms.
