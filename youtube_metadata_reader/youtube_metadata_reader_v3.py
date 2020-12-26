import shutil
from pathlib import Path

import httplib2
import sys
import csv
import re
from datetime import datetime, timedelta

from googleapiclient.discovery import build_from_document
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow

SITUATION_TXT = "situation.txt"

SOURCES = [
    #{"type": "channel", "id": "UCJ0dhh1UFNjzdCHa12IT-4A", "name": "Silverman", "cat": "lateshow"},
    #{"type": "channel", "id": "UCarEovlrD9QY-fy-Z6apIDQ", "name": "Minhaj", "cat": "lateshow"},
    {"type": "channel", "id": "UC3XTzVzaHQEd30rQbuvCtTQ", "name": "Oliver", "cat": "lateshow"},
    {"type": "channel", "id": "UCwWhs_6x42TyRM4Wstoq8HA", "name": "Noah", "cat": "lateshow"},
    {"type": "channel", "id": "UC18vz5hUUqxbGvym9ghtX_w", "name": "Bee", "cat": "lateshow"},
    {"type": "channel", "id": "UCVTyTA7-g9nopHeHbeuvpRA", "name": "Meyers", "cat": "lateshow"},
    {"type": "channel", "id": "UCMtFAi84ehTSYSE9XoHefig", "name": "Colbert", "cat": "lateshow"},
    {"type": "channel", "id": "UCy6kyFxaMqGtpE3pQTflK8A", "name": "Maher", "cat": "lateshow"},
    {"type": "channel", "id": "UC8-Th83bH_thdKZDJCrn88g", "name": "Fallon", "cat": "lateshow"},
    {"type": "channel", "id": "UCi7GJNg51C3jgmYTUwqoUXA", "name": "Conan", "cat": "lateshow"},
    {"type": "channel", "id": "UCJ0uqCI0Vqr2Rrt1HseGirg", "name": "Corden", "cat": "lateshow"},
    {"type": "channel", "id": "UCa6vGFO9ty8v5KZJXQxdhaw", "name": "Kimmel", "cat": "lateshow"},
    {"type": "channel", "id": "UCqFzWxSCi39LnW1JKFR3efg", "name": "SNL", "cat": "lateshow"},
    {"type": "channel", "id": "UC2gzy_aI-luPtEpL-GzQP6w", "name": "Singh", "cat": "lateshow"},
    #{"type": "channel", "id": "UCXIJgqnII2ZOINSWNOGFThA", "name": "FoxNews", "cat": "news"},
    #{"type": "channel", "id": "UCaXkIU1QidjPwiAYu6GcHjg", "name": "MSNBC", "cat": "news"},
    #{"type": "channel", "id": "UCupvZG-5ko_eiXAupbDfxWw", "name": "CNN", "cat": "news"},
    #{"type": "channel", "id": "UC8p1vwvWtl6T73JiExfWs1g", "name": "CBSNews", "cat": "news"},
    #{"type": "channel", "id": "UCeY0bbntWzzVIaj2z3QigXg", "name": "NBCNews", "cat": "news"},
    #{"type": "channel", "id": "UCBi2mrWuNuyYy4gbM6fU18Q", "name": "ABCNews", "cat": "news"},
    #{"type": "channel", "id": "UCzuqE7-t13O4NIDYJfakrhw", "name": "DemocracyNow", "cat": "onlineNewsComment"},
    #{"type": "channel", "id": "UC1yBKRuGpC1tSM73A0ZjYjQ", "name": "TheYoungTurks", "cat": "onlineNewsComment"},
    #{"type": "channel", "id": "UCLXo7UDZvByw2ixzpQCufnA", "name": "Vox", "cat": "onlineNewsComment"},
    #{"type": "channel", "id": "UCvsye7V9psc-APX6wV1twLg", "name": "AlexJones", "cat": "pundit"},
]


DAY_INCR = 2
DT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
CLIENT_SECRETS_FILE = "client_secrets10.json"
YOUTUBE_READ_WRITE_SSL_SCOPE = "https://www.googleapis.com/auth/youtube.force-ssl"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
MISSING_CLIENT_SECRETS_MESSAGE = "WARNING: Please configure OAuth 2.0"
CSV_PATH = "/Users/paulineziserman/GoogleDrive/these_db/csv/2012/2012_raw/"


def add_one_month(dt0):
    dt1 = dt0.replace(day=1)
    dt2 = dt1 + timedelta(days=32)
    dt3 = dt2.replace(day=1)
    return dt3


def get_authenticated_service():
    args = argparser.parse_args()
    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, scope=YOUTUBE_READ_WRITE_SSL_SCOPE,
                                   message=MISSING_CLIENT_SECRETS_MESSAGE)
    storage = Storage("%s-oauth2.json" % sys.argv[0])
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage, args)
    with open("youtube-v3-discoverydocument.json", "r") as f:
        doc = f.read()
        return build_from_document(doc, http=credentials.authorize(httplib2.Http()))


def get_videos(youtube_api, source, d_from, d_to):
    result = youtube_api.search().list(
        part="snippet",
        channelId=source["id"],
        publishedAfter=d_from.strftime(DT_FORMAT),
        publishedBefore=d_to.strftime(DT_FORMAT),
        safeSearch="none",
        type="video",
        maxResults=50
    ).execute()
    items = result["items"]
    print("get_videos. source: " + source["name"] + " from:" + d_from.strftime(DT_FORMAT) + " to:" + d_to.strftime(
        DT_FORMAT) + " : " + str(len(items)))
    return items


def get_stats(youtube_api, video_id):
    result = youtube_api.videos().list(
        part="contentDetails,statistics",
        id=video_id
    ).execute()
    return result["items"][0]


def get_comments(youtube_api, video_id, page_token):
    try:
        result = youtube_api.commentThreads().list(
            part="snippet",
            videoId=video_id,
            pageToken=page_token,
            # order="relevance",
            textFormat="plainText",
            maxResults=100
        ).execute()
        return result, result["items"], result.get("nextPageToken", False)
    except:
        print("get_comments: API error. videoId:" + video_id)
        return None, None, None


def clean_str(dirty_string):
    coding_string = str(dirty_string.encode("ascii", "ignore"))
    clean1 = re.sub(r'\s+', ' ', coding_string).strip()
    clean2 = re.sub(r',+', ';', clean1)
    res = clean2[2:-1]
    return res


def read_video(youtube_api, video_writer, source, video):
    res_stat = get_stats(youtube_api, video["id"]["videoId"])
    details = res_stat["contentDetails"]
    stats = res_stat["statistics"]

    if "commentCount" not in stats:
        comment_count = 0
        comment_lock = True
    else:
        comment_count = stats["commentCount"]
        comment_lock = False
    if "likeCount" not in stats:
        like_count = 0
    else:
        like_count = stats["likeCount"]
    if "dislikeCount" not in stats:
        dislike_count = 0
    else:
        dislike_count = stats["dislikeCount"]

    video_writer.writerow([source["cat"],
                           source["name"],
                           video["id"]["videoId"],
                           clean_str(video["snippet"]["title"]),
                           clean_str(video["snippet"]["description"]),
                           clean_str(video["snippet"]["publishedAt"]),
                           details["duration"],
                           stats["viewCount"],
                           like_count,
                           dislike_count,
                           comment_count,
                           comment_lock
                           ])


def read_comments(youtube_api, comment_writer, video):
    print("    read_comments START. video id:" + video["id"]["videoId"])
    page_token = None
    i = 0
    for _ in range(150):
        if page_token != False:
            res, comments, page_token = get_comments(youtube_api, video["id"]["videoId"], page_token)
            if res is None:
                break
            for comment in comments:
                clc = comment["snippet"]["topLevelComment"]["snippet"]
                if "authorChannelId" in clc:
                    author_id = clc["authorChannelId"]["value"]
                else:
                    author_id = "UNKNOWN"
                comment_writer.writerows([[comment["snippet"]["videoId"],
                                           comment["snippet"]["topLevelComment"]["id"], # comment id
                                           author_id,
                                           clean_str(clc["authorDisplayName"]),
                                           clean_str(clc["textDisplay"]),
                                           comment["snippet"]["totalReplyCount"],
                                           clc["likeCount"],
                                           clean_str(clc["publishedAt"])
                                           ]])
                i += 1
                if i % 1000 == 0:
                    print("        read_comments. Nbr : " + str(i))


def compute_left_and_right(d_current, d_to):
    left = d_current
    dif = d_to - left
    dif_days = dif.days
    if dif_days < DAY_INCR:
        right = d_to
    else:
        right = d_current + timedelta(days=DAY_INCR, seconds=-1)
    d_current = d_current + timedelta(days=DAY_INCR)
    return d_current, left, right


def get_file_name(file_type, source, period):
    return CSV_PATH + file_type + source["name"] + "_" + period["period"] + ".tmp"


def get_csv_video(video_file):
    video_writer = csv.writer(video_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    video_writer.writerow(["cat", "source", "videoId", "videoTitle", "videoDesc",
                           "videoPublishedAt", "duration", "viewCount", "likeCount", "dislikeCount",
                           "commentCount", "commentLock"])
    return video_writer


def get_csv_comment(comment_file):
    comment_writer = csv.writer(comment_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    comment_writer.writerow(["videoId", "commentId", "authorId", "author", "text", "replies", "likes", "publishedAt"])
    return comment_writer


def is_done(last_situation, source, period):
    if last_situation is None:
        return False
    period_current = period["period"]
    source_current = source["name"]
    for s in last_situation:
        if s[0] == period_current and s[1] == source_current:
            return True
    return False


def move_tmp_to_csv(file_type, source, period):
    tmp_name = CSV_PATH + file_type + source["name"] + "_" + period["period"] + ".tmp"
    csv_name = CSV_PATH + file_type + source["name"] + "_" + period["period"] + ".csv"
    shutil.move(tmp_name, csv_name)


def read_source(youtube_api, last_situation, source, period):
    print("read_source START. Period: " + period["period"] + " source: " + source["name"])
    with open(get_file_name("videos_", source, period), "w") as video_file:
        video_writer = get_csv_video(video_file)
        with open(get_file_name("comments_", source, period), "w") as comment_file:
            comment_writer = get_csv_comment(comment_file)
            d_current = period["from"]
            while d_current < period["to"]:
                d_current, left, right = compute_left_and_right(d_current, period["to"])
                videos = get_videos(youtube_api, source, left, right)
                for video in videos:
                    read_video(youtube_api, video_writer, source, video)
                    read_comments(youtube_api, comment_writer, video)
                    if video["id"]["videoId"] == "gcSwBR6l8QE":
                        import pdb
                        pdb.set_trace()

    move_tmp_to_csv("videos_", source, period)
    move_tmp_to_csv("comments_", source, period)
    write_situation(last_situation, period, source)


def get_last_situation():
    last_situation_file = Path(SITUATION_TXT)
    last = None
    situations = []
    if last_situation_file.is_file():
        with open(SITUATION_TXT, "r") as last_situation_file:
            csv_reader_situation = csv.reader(last_situation_file, delimiter=',')
            for line in csv_reader_situation:
                if len(line) >= 2:
                    situations.append(line)
                    last = line
    print("Last : " + str(last))
    return situations


def write_situation(last_situation, period, source):
    with open(SITUATION_TXT, "a") as situation_file:
        situation_writer = csv.writer(situation_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        situation_writer.writerow([period["period"], source["name"], str(datetime.now())])
    last_situation.append([period["period"], source["name"], str(datetime.now())])


def read_sources():
    youtube_api = get_authenticated_service()
    last_situation = get_last_situation()
    d_current = FROM
    while d_current < TO:
        period = {"period": str(d_current.year) + "-" + str(d_current.month).zfill(2),
                  "from": d_current,
                  "to": add_one_month(d_current) + timedelta(seconds=-1)}
        for source in SOURCES:
            if is_done(last_situation, source, period):
                print("read_source DONE. Period: " + period["period"] + " source: " + source["name"])
            else:
                read_source(youtube_api, last_situation, source, period)
        d_current = add_one_month(d_current)


FROM = datetime(2020, 11, 1)
TO = datetime(2020, 12, 1) + timedelta(seconds=-1)

read_sources()
