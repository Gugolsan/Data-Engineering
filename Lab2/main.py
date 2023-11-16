import os
import requests
from zipfile import ZipFile
import aiohttp
import asyncio
#from io import BytesIO

# List of download URIs
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

# Create the "downloads" directory if it doesn't exist
if not os.path.exists("downloads"):
    os.makedirs("downloads")


def download_and_extract(uri):
    try:
        # Download the file
        response = requests.get(uri)
        response.raise_for_status()

        # Extract filename from the URI
        filename = uri.split("/")[-1]

        # Save the zip file
        zip_path = os.path.join("downloads", filename)
        with open(zip_path, "wb") as zip_file:
            zip_file.write(response.content)

        # Extract contents of the zip file
        with ZipFile(zip_path, "r") as zip_ref:
            # Assuming the CSV file is the first file in the zip
            csv_filename = zip_ref.namelist()[0]
            csv_content = zip_ref.read(csv_filename)

            # Save the CSV file
            csv_path = os.path.join("downloads", csv_filename)
            with open(csv_path, "wb") as csv_file:
                csv_file.write(csv_content)

        # Remove the zip file
        os.remove(zip_path)

        print(f"Processed file: {uri}")

    except Exception as e:
        print(f"Error processing file {uri}: {str(e)}")


async def download_and_extract_async(session, uri):
    try:
        # Download the file
        async with session.get(uri) as response:
            response.raise_for_status()
            data = await response.read()

            # Extract filename from the URI
            filename = uri.split("/")[-1]

            # Save the zip file
            zip_path = os.path.join("downloads", filename)
            with open(zip_path, "wb") as zip_file:
                zip_file.write(data)

            # Extract contents of the zip file
            with ZipFile(zip_path, "r") as zip_ref:
                # Assuming the CSV file is the first file in the zip
                csv_filename = zip_ref.namelist()[0]
                csv_content = zip_ref.read(csv_filename)

                # Save the CSV file
                csv_path = os.path.join("downloads", csv_filename)
                with open(csv_path, "wb") as csv_file:
                    csv_file.write(csv_content)

            # Remove the zip file
            os.remove(zip_path)

            print(f"Processed file: {uri}")

    except Exception as e:
        print(f"Error processing file {uri}: {str(e)}")


def main_sync():
    # Download and extract each URI
    for uri in download_uris:
        download_and_extract(uri)


async def main_async():
    async with aiohttp.ClientSession() as session:
        tasks = [download_and_extract_async(session, uri) for uri in download_uris]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    # Choose either synchronous or asynchronous version
    main_sync()
    # asyncio.run(main_async())
