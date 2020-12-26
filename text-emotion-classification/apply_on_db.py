from datetime import datetime
import keras
import pickle
from keras.preprocessing.sequence import pad_sequences
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session
from db_declaration import Base, Source, Video, Comment, Author

VERSION = 0
SQL_FILE = 'dbtest.sql'
DB_PATH = ""
WEIGHT_LIMIT = 0.5
MAX_NB_WORDS = 40000 # max no. of words for tokenizer
MAX_SEQUENCE_LENGTH = 30 # max length of text (words) including padding
VALIDATION_SPLIT = 0.2
EMBEDDING_DIM = 200 # embedding dimensions for word vectors (word2vec/GloVe)
CLASSES = ["neutral", "happy", "sad", "hate","anger"]


class Predictor:
    def __init__(self):
        self.model = keras.models.load_model("checkpoint-0.917.h5")
        with open('tokenizer.pickle', 'rb') as handle:
            self.tokenizer = pickle.load(handle)
        print("End Init Predictor")

    def eval(self, s):
        #start = datetime.now()
        sequences_test = self.tokenizer.texts_to_sequences([s])
        data_int_t = pad_sequences(sequences_test, padding='pre', maxlen=(MAX_SEQUENCE_LENGTH - 5))
        data_test = pad_sequences(data_int_t, padding='post', maxlen=(MAX_SEQUENCE_LENGTH))
        y_prob = self.model.predict(data_test)
        for n, prediction in enumerate(y_prob):
            pred = y_prob.argmax(axis=-1)[n]
            w = float(prediction[pred])
            #duration = datetime.now() - start
            #print("d:" + str(duration))
            return CLASSES[pred], w


def get_count(q):
    count_q = q.statement.with_only_columns([func.count()]).order_by(None)
    count = q.session.execute(count_q).scalar()
    return count


class Main:
    def __init__(self):
        self.engine = create_engine('sqlite:///' + DB_PATH + SQL_FILE)
        Base.metadata.bind = self.engine
        self.session = Session()
        self.predictor = Predictor()
        connection = self.engine.connect()
        connection.execute(text('PRAGMA synchronous = OFF'))
        connection.execute(text('PRAGMA journal_mode = MEMORY'))

    def run_comment(self):
        query = self.session.query(Comment).filter(Comment.sentiment == "")
        count = get_count(query)
        print("Start eval sentiment on comments: " + str(count))
        n = 0
        ok = 0
        nok = 0
        start = datetime.now()
        for c in query:
            sentiment, weight = self.predictor.eval(c.text)
            if weight > WEIGHT_LIMIT:
                c.sentiment = sentiment
                c.weight = weight
                ok += 1
            else:
                c.sentiment = "UNKNOWN"
                c.weight = weight
                nok += 1
            self.session.commit()
            n += 1
            if n % 1000 == 0:
                elapsed = datetime.now() - start
                left = ((count - n) / n) * elapsed
                print(" Comment processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok) + " Rest:" + str(count - n) + " Elapsed:" + str(elapsed)[:-7] + " Left:" + str(left)[:-7])
            elif n % 100 == 0:
                print(".", end='', flush=True)
        print(" Comment processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok))

    def run_video(self):
        query = self.session.query(Video).filter(Video.version != VERSION)
        count = get_count(query)
        print("Start synthesis sentiment on videos: " + str(count))
        n = 0
        ok = 0
        nok = 0
        start = datetime.now()
        for v in query:
            v.neutral = 0
            v.happy = 0
            v.sad = 0
            v.hate = 0
            v.anger = 0
            for c in Session().query(Comment).filter(Comment.videoId == v.id):
                if c.sentiment == "neutral":
                    v.neutral += 1
                elif c.sentiment == "happy":
                    v.happy += 1
                elif c.sentiment == "sad":
                    v.sad += 1
                elif c.sentiment == "hate":
                    v.hate += 1
                elif c.sentiment == "anger":
                    v.anger += 1
            v.version = VERSION
            if v.happy + v.neutral + v.sad + v.hate + v.anger > 0:
                ok += 1
            else:
                nok += 1
            self.session.commit()
            n += 1
            if n % 1000 == 0:
                elapsed = datetime.now() - start
                left = ((count - n) / n) * elapsed
                print(" Video processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok) + " Rest:" + str(count - n) + " Elapsed:" + str(elapsed)[:-7] + " Left:" + str(left)[:-7])
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
            a.neutral = 0
            a.happy = 0
            a.sad = 0
            a.hate = 0
            a.anger = 0
            for c in Session().query(Comment).filter(Comment.authorId == a.id):
                if c.sentiment == "neutral":
                    a.neutral += 1
                elif c.sentiment == "happy":
                    a.happy += 1
                elif c.sentiment == "sad":
                    a.sad += 1
                elif c.sentiment == "hate":
                    a.hate += 1
                elif c.sentiment == "anger":
                    a.anger += 1
            a.version = VERSION
            if a.happy + a.neutral + a.sad + a.hate + a.anger > 0:
                ok += 1
            else:
                nok += 1
            self.session.commit()
            n += 1
            if n % 1000 == 0:
                elapsed = datetime.now() - start
                left = ((count - n) / n) * elapsed
                print(" Author processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok) + " Rest:" + str(count - n) + " Elapsed:" + str(elapsed)[:-7] + " Left:" + str(left)[:-7])
            elif n % 100 == 0:
                print(".", end='', flush=True)
        print(" Author processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok))

    def run(self):
        self.run_comment()
        self.run_video()
        self.run_author()


main = Main()
main.run()
