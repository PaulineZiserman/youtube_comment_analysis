from datetime import datetime

from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session
from db_declaration import Base, Source, Video, Comment, Author

VERSION = 17
SQL_FILE = 'LNSyoutube_v17.db'
DB_PATH = "/Users/paulineziserman/GoogleDrive/these_db/Bases/"


def get_count(q):
    count_q = q.statement.with_only_columns([func.count()]).order_by(None)
    count = q.session.execute(count_q).scalar()
    return count


class Main:
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
        # if self.sql('CREATE INDEX ix_video_version ON video (version)'):
        #    self.sql('UPDATE video SET version = -1;')
        # if self.sql('CREATE INDEX ix_author_version ON author (version)'):
        #    self.sql('UPDATE author SET version = -1;')

    def run_video(self):
        query = self.session.query(Video).filter(Video.version != VERSION)
        count = get_count(query)
        print("Start synthesis sentiment on videos: " + str(count))
        n = 0
        ok = 0
        nok = 0
        start = datetime.now()
        for v in query:
            neutral = 0
            happy = 0
            sad = 0
            hate = 0
            anger = 0
            sentiment_nbr = 0
            unknown = 0
            for c in self.session.query(Comment).filter(Comment.video_id == v.id):
                if c.sentiment == "neutral":
                    neutral += 1
                    sentiment_nbr += 1
                elif c.sentiment == "happy":
                    happy += 1
                    sentiment_nbr += 1
                elif c.sentiment == "sad":
                    sad += 1
                    sentiment_nbr += 1
                elif c.sentiment == "hate":
                    hate += 1
                    sentiment_nbr += 1
                elif c.sentiment == "anger":
                    anger += 1
                    sentiment_nbr += 1
                else:
                    unknown += 1
            if sentiment_nbr + unknown == 0:
                nok += 1
            else:
                ok += 1
                if sentiment_nbr > 0:
                    neutral = neutral / sentiment_nbr
                    happy = happy / sentiment_nbr
                    sad = sad / sentiment_nbr
                    hate = hate / sentiment_nbr
                    anger = anger / sentiment_nbr
                sql = 'UPDATE video SET neutral={0},happy={1},sad={2},hate={3},anger={4},unknown={5},sentiment_nbr={6},version={7} WHERE id="{8}";'.format(
                    str(neutral), str(happy), str(sad), str(hate), str(anger), str(unknown), str(sentiment_nbr), str(VERSION), v.id)
                self.sql(sql)
            n += 1
            if n % 1000 == 0:
                elapsed = datetime.now() - start
                left = ((count - n) / n) * elapsed
                print(" Video processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok) + " Rest:" + str(
                    count - n) + " Elapsed:" + str(elapsed)[:-7] + " Left:" + str(left)[:-7])
            elif n % 100 == 0:
                print(".", end='', flush=True)
        print(" Video processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok))

    def run_author(self):
        query = self.session.query(Author).filter(Author.version != VERSION)
        count = get_count(query)
        print("Start synthesis sentiment on authors: " + str(count))
        n = 0
        ok = 0
        nok = 0
        start = datetime.now()
        for a in query:
            neutral = 0
            happy = 0
            sad = 0
            hate = 0
            anger = 0
            unknown = 0
            sentiment_nbr = 0
            for c in self.session.query(Comment).filter(Comment.author_id == a.id):
                if c.sentiment == "neutral":
                    sentiment_nbr += 1
                    neutral += 1
                elif c.sentiment == "happy":
                    sentiment_nbr += 1
                    happy += 1
                elif c.sentiment == "sad":
                    sentiment_nbr += 1
                    sad += 1
                elif c.sentiment == "hate":
                    sentiment_nbr += 1
                    hate += 1
                elif c.sentiment == "anger":
                    sentiment_nbr += 1
                    anger += 1
                else:
                    unknown += 1
            if sentiment_nbr + unknown == 0:
                nok += 1
            else:
                ok += 1
                if sentiment_nbr > 0:
                    neutral = neutral / sentiment_nbr
                    happy = happy / sentiment_nbr
                    sad = sad / sentiment_nbr
                    hate = hate / sentiment_nbr
                    anger = anger / sentiment_nbr
            sql = 'UPDATE author SET neutral={0},happy={1},sad={2},hate={3},anger={4},unknown={5},sentiment_nbr={6},version={7} WHERE id="{8}";'.format(
                str(neutral), str(happy), str(sad), str(hate), str(anger), str(unknown), str(sentiment_nbr), str(VERSION), a.id)
            self.sql(sql)
            n += 1
            if n % 1000 == 0:
                elapsed = datetime.now() - start
                left = ((count - n) / n) * elapsed
                print(" Author processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok) + " Rest:" + str(
                    count - n) + " Elapsed:" + str(elapsed)[:-7] + " Left:" + str(left)[:-7])
            elif n % 100 == 0:
                print(".", end='', flush=True)
        print(" Author processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok))

    def run(self):
        self.run_video()
        self.run_author()
        print("END :)")


main = Main()
main.run()
