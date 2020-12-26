from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from db_declaration import Base, Source, Video, Comment, Author

SQL_FILE = 'dbtest.sql'
DB_PATH = ""

engine = create_engine('sqlite:///' + DB_PATH + SQL_FILE)
Base.metadata.bind = engine

session = Session()

for c in session.query(Comment).filter(Comment.sentiment == "test"):
    c.sentiment = ""
    session.commit()


