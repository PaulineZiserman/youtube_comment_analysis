import sys
from pathlib import Path

import isodate as isodate
from sqlalchemy import *
import csv
from datetime import datetime
from os import listdir
from os.path import isfile, join

DB_PATH = "/Users/paulineziserman/GoogleDrive/these_db/Bases/"
SQL_FILE = 'LNSyoutube_v8.db'
DBSITUATION = "LNSyoutube_v8.txt"
CSV_PATH_COMMENTS = "/Users/paulineziserman/GoogleDrive/these_db/csv/2001/2001_comments_with_sentiments/"
CSV_PATH_OTHERS = "/Users/paulineziserman/GoogleDrive/these_db/csv/2001/2001_raw/"
DT_FORMAT = "%Y-%m-%dT%H:%M:%S"
DT_FORMAT_CHANNEL = "%d/%m/%Y"


def get_int(s):
    return int(s)


def get_float(s):
    return float(s)


def get_sec(s):
    return int(isodate.parse_duration(s).total_seconds())


def get_date(s):
    return datetime.strptime(s, DT_FORMAT_CHANNEL)


def get_datetime(s):
    return datetime.strptime(s[:-5], DT_FORMAT)


def get_bool(s):
    if s == 'False':
        return False
    return True


def print_after_read(nbr, file, errors, ok):
    if errors > 0 and ok > 0:
        msg = " --> Errors: " + str(errors) + " New lines: " + str(ok)
    elif errors == 0 and ok == 0:
        msg = " --> Empty file"
    else:
        msg = " --> New lines: " + str(ok)
    print(msg)


def print_error(exception, errors, row):
    if errors == 0:
        print("", file=sys.stderr)
    print("  error (" + format(exception) + ") :" + str(row), file=sys.stderr)
    errors += 1
    return errors


def incr_ok(ok):
    ok += 1
    if ok % 1000 == 0:
        print('.', end='', flush=True)
    return ok


def get_last_situation():
    last_situation_file = Path(DBSITUATION)
    situations = []
    if last_situation_file.is_file():
        with open(DBSITUATION, "r") as last_situation_file:
            csv_reader_situation = csv.reader(last_situation_file, delimiter=',')
            for line in csv_reader_situation:
                if len(line) >= 2:
                    situations.append(line)
    return situations


class Db:
    def __init__(self):
        self.engine = create_engine('sqlite:///' + DB_PATH + SQL_FILE)
        self.metadata = MetaData()
        self.table_source = Table('source', self.metadata,
                                  Column('name', String(25), primary_key=True, nullable=False),
                                  Column('cat', String(25), nullable=False),
                                  Column('creation', DateTime(), nullable=False),
                                  Column('followers', Integer(), nullable=False),
                                  Column('views', Integer(), nullable=False),
                                  )
        self.table_video = Table('video', self.metadata,
                                 Column('category', String(10), nullable=False),
                                 Column('source_name', String(25), ForeignKey("source.name"), nullable=False),
                                 Column('id', String(20), primary_key=True, nullable=False),
                                 Column('title', String(160), nullable=False),
                                 Column('description', String(500), nullable=False),
                                 Column('at', DateTime(), nullable=False),
                                 Column('duration', Integer(), nullable=False),
                                 Column('views', Integer(), nullable=False),
                                 Column('likes', Integer(), nullable=False),
                                 Column('dislikes', Integer(), nullable=False),
                                 Column('comments', Integer(), nullable=False),
                                 Column('comment_lock', Boolean(), nullable=False),
                                 Column('happy', Integer()),
                                 Column('sad', Integer()),
                                 Column('anger', Integer()),
                                 Column('hate', Integer()),
                                 Column('neutral', Integer()),
                                 Column('version', Integer()),
                                 )
        self.table_comment = Table('comment', self.metadata,
                                   Column('video_id', String(20), ForeignKey("video.id"), nullable=False),
                                   Column('id', String(45), primary_key=True, nullable=False),
                                   Column('author_id', String(20), ForeignKey("author.id"), nullable=False),
                                   Column('text', String(1700), nullable=False),
                                   Column('replies', Integer(), nullable=False),
                                   Column('likes', Integer(), nullable=False),
                                   Column('at', DateTime(), nullable=False),
                                   Column('sentiment', String(14)),
                                   Column('weight', Integer()),
                                   )
        self.table_author = Table('author', self.metadata,
                                  Column('id', String(20), primary_key=True, nullable=False),
                                  Column('name', String(60), nullable=False),
                                  Column('happy', Integer()),
                                  Column('sad', Integer()),
                                  Column('anger', Integer()),
                                  Column('hate', Integer()),
                                  Column('neutral', Integer()),
                                  Column('version', Integer()),
                                  )
        self.metadata.create_all(self.engine)
        self.conn = self.engine.connect()
        self.conn.execute(text('PRAGMA synchronous = OFF'))
        self.conn.execute(text('PRAGMA journal_mode = MEMORY'))
        self.situation = get_last_situation()

    def write_situation(self, file):
        with open(DBSITUATION, "a") as situation_file:
            situation_writer = csv.writer(situation_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            situation_writer.writerow([file, str(datetime.now())])
        self.situation.append([file, str(datetime.now())])

    def is_done(self, file):
        if self.situation is None:
            return False
        for s in self.situation:
            if s[0] == file:
                return True
        return False

    def read_all(self):
        for f in listdir(CSV_PATH_OTHERS):
            if isfile(join(CSV_PATH_OTHERS, f)):
                if f.startswith("channel") and f.endswith("csv") and not self.is_done(f):
                    self.read_sources(f)
                    self.write_situation(f)
        for f in listdir(CSV_PATH_OTHERS):
            if isfile(join(CSV_PATH_OTHERS, f)) and f.endswith("csv") and not self.is_done(f):
                if f.startswith("video"):
                    self.read_videos(f)
                    self.write_situation(f)
        for f in listdir(CSV_PATH_COMMENTS):
            if isfile(join(CSV_PATH_COMMENTS, f)) and f.endswith("csv") and not self.is_done(f):
                if f.startswith("comment"):
                    self.read_comments(f)
                    self.write_situation(f)

    def read_sources(self, file_name):
        with open(CSV_PATH_OTHERS + file_name) as file_videos:
            print(str(datetime.now()) + " : " + file_name + " - ", end='', flush=True)
            csv_reader = csv.reader(file_videos, delimiter=',')
            next(csv_reader, None)
            ok = 0
            errors = 0
            for row in csv_reader:
                try:
                    ins = self.table_source.insert()
                    self.conn.execute(ins,
                                      name=row[0],
                                      cat=row[1],
                                      creation=get_date(row[2]),
                                      followers=get_int(row[3]),
                                      views=get_int(row[4]),
                                      )
                    ok = incr_ok(ok)
                except Exception as e:
                    errors = print_error(e, errors, row)
        print_after_read(len(self.situation), file_name, errors, ok)

    def read_videos(self, file_name):
        with open(CSV_PATH_OTHERS + file_name) as file_videos:
            print(str(datetime.now()) + " : " + file_name + " - ", end='', flush=True)
            csv_reader = csv.reader(file_videos, delimiter=',')
            next(csv_reader, None)
            ok = 0
            errors = 0
            for row in csv_reader:
                try:
                    ins = self.table_video.insert()
                    self.conn.execute(ins,
                                      category=row[0],
                                      source_name=row[1],
                                      id=row[2],
                                      title=row[3],
                                      description=row[4],
                                      at=get_datetime(row[5]),
                                      duration=get_sec(row[6]),
                                      views=get_int(row[7]),
                                      likes=get_int(row[8]),
                                      dislikes=get_int(row[9]),
                                      comments=get_int(row[10]),
                                      comment_lock=get_bool(row[11]),
                                      happy=0,
                                      sad=0,
                                      anger=0,
                                      hate=0,
                                      neutral=0,
                                      version=-1
                                      )
                    ok = incr_ok(ok)
                except Exception as e:
                    errors = print_error(e, errors, row)
        print_after_read(len(self.situation), file_name, errors, ok)

    def add_author(self, author_id, author_name):
        try:
            ins = self.table_author.insert()
            self.conn.execute(ins,
                              id=author_id,
                              name=author_name,
                              happy=0,
                              sad=0,
                              anger=0,
                              hate=0,
                              neutral=0,
                              version=-1
                              )
        except:
            pass

    def read_comments(self, file_name):
        with open(CSV_PATH_COMMENTS + file_name) as file_videos:
            print(str(datetime.now()) + " : " + file_name + " - ", end='', flush=True)
            csv_reader = csv.reader(file_videos, delimiter=',')
            next(csv_reader, None)
            ok = 0
            errors = 0
            for row in csv_reader:
                try:
                    self.add_author(row[2], row[3])
                    ins = self.table_comment.insert()
                    self.conn.execute(ins,
                                      video_id=row[0],
                                      id=row[0]+":"+row[1],
                                      author_id=row[2],
                                      text=row[4],
                                      replies=get_int(row[5]),
                                      likes=get_int(row[6]),
                                      at=get_datetime(row[7]),
                                      sentiment=row[8],
                                      weight=-get_float(row[9])
                                      )
                    ok = incr_ok(ok)
                except Exception as e:
                    errors = print_error(e, errors, row)
            print_after_read(len(self.situation), file_name, errors, ok)


db = Db()
db.read_all()
