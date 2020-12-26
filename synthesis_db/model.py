from datetime import datetime
import html
import csv
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session
from db_declaration import Base, Source, Video, Comment, Author

import fire

VERSION = 14
SQL_FILE = 'LNSyoutube_v11.db'
DB_PATH = "/Users/paulineziserman/GoogleDrive/these_db/Bases/"

class Model:
    def sql(self, command):
        try:
            self.connection.execute(text(command))
            return True
        except Exception as e:
            print("Error:" + str(e))
            return False

    def __init__(self):
        self.engine = create_engine('sqlite:///' + DB_PATH + SQL_FILE)
        Base.metadata.bind = self.engine
        self.session = Session()
        self.connection = self.engine.connect()
        self.sql('PRAGMA synchronous = OFF')
        self.sql('PRAGMA journal_mode = MEMORY')

    def clean_da_base(self):
        query = self.session.query(Video)
        for v in query:
            for c in self.session.query(Comment).filter(Comment.video_id == v.id):
                c.text = html.unescape(c.text)
            for a in self.session.query(Author).filter(Author.id == v.author_id):
                a.title = html.unescape(a.title)
                a.description = html.unescape(a.description)
                a.name = html.unescape(a.name)

        self.session.commit()

    def update_segment_from_names_csv(self, csv_path, segment):
        videos = self.session.query(Video)
        with open(csv_path) as file:
            for title in csv.reader(file):
                i = 0
                for video in videos.filter(Video.title == title[0]):
                    i += 1
                    video.segment = segment
                if i == 0:
                    print(title)

        self.session.commit()


if __name__ == '__main__':
  fire.Fire(Model)
