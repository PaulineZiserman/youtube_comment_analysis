from datetime import datetime
import html
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import StaleDataError

from db_declaration import Base, Source, Video, Comment, Author

VERSION = 8
SQL_FILE = 'LNSyoutube_v8.db'
DB_PATH = "/Users/paulineziserman/GoogleDrive/these_db/Bases/"

class Cleaner:
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
            v.description = html.unescape(v.description)
            v.title = html.unescape(v.title)
            for c in self.session.query(Comment).filter(Comment.video_id == v.id):
                c.text = html.unescape(c.text)
            try:
                self.session.commit()
            except StaleDataError:
                import code;
                code.interact(local=dict(globals(), **locals()))

cleaner = Cleaner()
cleaner.clean_da_base()
