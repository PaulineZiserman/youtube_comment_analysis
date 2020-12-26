from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship

SQL_FILE = 'dbtest.sql'
DB_PATH = ""

Base = declarative_base()


class Source(Base):
    __tablename__ = 'source'
    name = Column(String(25), primary_key=True, nullable=False)
    cat = Column(String(25), nullable=False)
    creation = Column(DateTime(), nullable=False)
    followers = Column(Integer(), nullable=False)
    views = Column(Integer(), nullable=False)


class Video(Base):
    __tablename__ = 'video'
    category = Column(String(10), nullable=False)
    source_name = Column(String(25), ForeignKey("source.name"), nullable=False)
    id = Column(String(20), nullable=False)
    title = Column(String(160), primary_key=True, nullable=False)
    description = Column(String(500), nullable=False)
    at = Column(DateTime(), nullable=False)
    duration = Column(Integer(), nullable=False)
    views = Column(Integer(), nullable=False)
    likes = Column(Integer(), nullable=False)
    dislikes = Column(Integer(), nullable=False)
    comments = Column(Integer(), nullable=False)
    comment_lock = Column(Boolean(), nullable=False)
    happy = Column(Integer())
    sad = Column(Integer())
    anger = Column(Integer())
    hate = Column(Integer())
    neutral = Column(Integer())
    unknown = Column(Integer())
    version = Column(Integer())
    sentiment_nbr = Column(Integer())
    segment = Column(String(160))


class Comment(Base):
    __tablename__ = 'comment'
    video_id = Column(String(20), ForeignKey("video.id"), nullable=False)
    id = Column(String(45), primary_key=True, nullable=False)
    author_id = Column(String(20), ForeignKey("author.id"), nullable=False)
    text = Column(String(1700), nullable=False)
    replies = Column(Integer(), nullable=False)
    likes = Column(Integer(), nullable=False)
    at = Column(DateTime(), nullable=False)
    sentiment = Column(String(14))
    weight = Column(Integer())


class Author(Base):
    __tablename__ = 'author'
    id = Column(String(20), primary_key=True, nullable=False)
    name = Column(String(60), nullable=False)
    happy = Column(Integer())
    sad = Column(Integer())
    anger = Column(Integer())
    hate = Column(Integer())
    neutral = Column(Integer())
    unknown = Column(Integer())
    version = Column(Integer())
    sentiment_nbr = Column(Integer())


engine = create_engine('sqlite:///' + DB_PATH + SQL_FILE)
Base.metadata.create_all(engine)
