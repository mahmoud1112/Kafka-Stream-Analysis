import logging
import sys
import requests
from config import config
import json
from kafka import KafkaProducer
import matplotlib.pyplot as plt
import numpy as np



def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems",
                            params={
                                "key": google_api_key,
                                "playlistId": youtube_playlist_id,
                                "part": "contentDetails",
                                "pageToken": page_token
                            })

    payload = json.loads(response.text)

    logging.debug("GOT %s", payload)

    return payload

def fetch_videos_page(google_api_key, video_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos",
                            params={
                                "key": google_api_key,
                                "id": video_id,
                                "part": "snippet,statistics",
                                "pageToken": page_token
                            })

    payload = json.loads(response.text)

    logging.debug("GOT %s", payload)

    return payload
def fetch_playlist_items(google_api_key, youtube_playlist_id, page_token=None):
    payload = fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token)

    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_playlist_items(google_api_key, youtube_playlist_id, next_page_token)


def fetch_videos(google_api_key, youtube_playlist_id, page_token=None):
    payload = fetch_videos_page(google_api_key, youtube_playlist_id, page_token)

    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_videos(google_api_key, youtube_playlist_id, next_page_token)


def summarize_video(video):
    return {
        "video_id": video["id"],
        "title": video["snippet"]["title"],
        "views": int(video["statistics"].get("viewCount", 0)),
        "likes": int(video["statistics"].get("likeCount", 0)),
        "comments": int(video["statistics"].get("commentCount", 0)),
    }

def main():
    google_api_key = config["google_api_key"] # from config
    youtube_playlist_id = config["youtube_playlist_id"] #from config 
    producer = KafkaProducer(bootstrap_servers= ['localhost:9092'],value_serializer=json_serializer) #make producer 
    temp =[] #لو في تغيير بين القديمة والجديدة يبقي حصل تغيير والمفروض يبعته علي تليجرام بوت 
    for video_item in fetch_playlist_items(google_api_key, youtube_playlist_id): # playlist kolha
        video_id = video_item["contentDetails"]["videoId"]
        for video in fetch_videos(google_api_key, video_id):  # each vedio in playlist 
            temp.append({
                        "TITLE": video["snippet"]["title"], # snippet class in html of title 
                        "VIEWS": int(video["statistics"].get("viewCount", 0)),
                        "LIKES": int(video["statistics"].get("likeCount", 0)),
                        "COMMENTS": int(video["statistics"].get("commentCount", 0)),
                    })
    likes=0
    views=0
    comments=0        
    for i in temp:
        likes=likes+i['LIKES']
        views=views+i['VIEWS']
        comments=comments+i['COMMENTS']
    data = ['LIKES','VIEWS','COMMENTS']
    values = [likes,views,comments]
    fig = plt.figure(figsize = (10, 5))
# creating the bar plot
    plt.bar(data, values, color ='maroon',width = 0.4)
 
    plt.xlabel("play list data")
    plt.title("show all play list likes , views and comments")
    plt.show()

    print(temp) # in trminal 
    while 1 == 1: #3lshan tfdl sh3'ala 3la tool 3lshan lw 7sl ay ta3'eer fe el data t2olna we yb3tha fe el producer + update el temp 
        i=0
        for video_item in fetch_playlist_items(google_api_key, youtube_playlist_id):
            video_id = video_item["contentDetails"]["videoId"]
            for video in fetch_videos(google_api_key, video_id):
                if temp[i]["COMMENTS"] != int(video["statistics"].get("commentCount", 0)) or temp[i]["LIKES"] !=int(video["statistics"].get("likeCount", 0)) or temp[i]["VIEWS"] != int(video["statistics"].get("viewCount", 0)) :
                    producer.send("vid_list",value={ # vid list we created in consumer < e7na hncosume mn topic 
                        "TITLE": video["snippet"]["title"],
                        "VIEWS": int(video["statistics"].get("viewCount", 0)),
                        "LIKES": int(video["statistics"].get("likeCount", 0)),
                        "COMMENTS": int(video["statistics"].get("commentCount", 0)),
                    },)
                    temp[i]={
                        "TITLE": video["snippet"]["title"],
                        "VIEWS": int(video["statistics"].get("viewCount", 0)),
                        "LIKES": int(video["statistics"].get("likeCount", 0)),
                        "COMMENTS": int(video["statistics"].get("commentCount", 0)),
                    }
                i=i+1 

      

if __name__ == "__main__":
    sys.exit(main())