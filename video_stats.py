import requests
import json
import os
from dotenv import load_dotenv
from datetime import date

# Load API key securely from .env file
load_dotenv(dotenv_path='./.env')

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = 'MrBeast'
maxResults = 50


def get_playlist_id():
    """
    Get the channel's uploads playlist ID.
    Every YouTube channel has a special 'uploads' playlist that contains all its videos.
    """
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Extract the uploads playlist ID from channel details
        channel_items = data["items"][0]
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]['uploads']

        print(f"Channel Playlist ID: {channel_playlistId}")
        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e


def get_video_ids(playlistId):
    """
    Retrieve all video IDs from the uploads playlist.
    Handles pagination using nextPageToken until all videos are collected.
    """
    video_ids = []
    pageToken = None

    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}'

    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Collect video IDs from each page of results
            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            # Move to next page if available
            pageToken = data.get('nextPageToken')
            if not pageToken:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e


def extract_video_data(video_ids):
    """
    Fetch detailed metadata for each video ID.
    Splits video IDs into batches and calls videos.list endpoint.
    Extracts snippet, contentDetails, and statistics for each video.
    """
    extracted_data = []

    # Helper generator to split video IDs into batches
    def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size):
            yield video_id_list[video_id: video_id + batch_size]

    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Extract structured metadata for each video
            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contenDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contenDetails['duration'],
                    "viewCount": statistics.get('viewCount', None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None)
                }

                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    
    with open(file_path, "w", encoding = "utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)


# Pipeline execution:
if __name__ == "__main__":
    # 1. Get channel's uploads playlist ID
    playlistId = get_playlist_id()

    # 2. Get all video IDs from the playlist
    video_ids = get_video_ids(playlistId)

    # 3. Extract detailed metadata for each video
    video_data = extract_video_data(video_ids)
    
    # 4. Save the data in .json format
    save_to_json(video_data)