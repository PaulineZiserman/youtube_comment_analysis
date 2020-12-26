import csv
import time
from datetime import datetime
from os import listdir
from os.path import join, isfile
from pathlib import Path

import keras
import pickle
from keras.preprocessing.sequence import pad_sequences

WEIGHT_LIMIT = 0.5
BLOCK_SIZE = 5000 # block of text to process
MAX_NB_WORDS = 40000 # max no. of words for tokenizer
MAX_SEQUENCE_LENGTH = 30 # max length of text (words) including padding
VALIDATION_SPLIT = 0.2
EMBEDDING_DIM = 200 # embedding dimensions for word vectors (word2vec/GloVe)
CSV_PATH_INPUT = "/Users/paulineziserman/GoogleDrive/these_db/csv/2011/2011_raw/"
CSV_PATH_OUTPUT = "/Users/paulineziserman/GoogleDrive/these_db/csv/2011/2011_comments_with_sentiments/"
CLASSES = ["neutral", "happy", "sad", "hate", "anger"]
SITUATION = "situation_add_sentiment_to_csv.txt"


def get_last_situation():
    last_situation_file = Path(SITUATION)
    situations = []
    if last_situation_file.is_file():
        with open(SITUATION, "r") as last_situation_file:
            csv_reader_situation = csv.reader(last_situation_file, delimiter=',')
            for line in csv_reader_situation:
                if len(line) >= 2:
                    situations.append(line)
    return situations


def write_situation(last_situation, file):
    with open(SITUATION, "a") as situation_file:
        situation_writer = csv.writer(situation_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        situation_writer.writerow([file, str(datetime.now())])
    last_situation.append([file, str(datetime.now())])


def is_done(last_situation, file):
    if last_situation is None:
        return False
    for s in last_situation:
        if s[0] == file:
            return True
    return False


class Predictor:
    def __init__(self):
        self.model = keras.models.load_model("checkpoint-0.917.h5")
        with open('tokenizer.pickle', 'rb') as handle:
            self.tokenizer = pickle.load(handle)
        print("End Init Predictor")

    def eval(self, s):
        sequences_test = self.tokenizer.texts_to_sequences([s])
        data_int_t = pad_sequences(sequences_test, padding='pre', maxlen=(MAX_SEQUENCE_LENGTH - 5))
        data_test = pad_sequences(data_int_t, padding='post', maxlen=MAX_SEQUENCE_LENGTH)
        y_prob = self.model.predict(data_test)
        for n, prediction in enumerate(y_prob):
            pred = y_prob.argmax(axis=-1)[n]
            w = float(prediction[pred])
            return CLASSES[pred], w

    def eval_block(self, b):
        sequences_test = self.tokenizer.texts_to_sequences(b)
        data_int_t = pad_sequences(sequences_test, padding='pre', maxlen=(MAX_SEQUENCE_LENGTH - 5))
        data_test = pad_sequences(data_int_t, padding='post', maxlen=MAX_SEQUENCE_LENGTH)
        return self.model.predict(data_test)


class Main:
    def __init__(self):
        self.predictor = Predictor()

    def run(self, channel):
        do_something = False
        situations = get_last_situation()
        for f in listdir(CSV_PATH_INPUT):
            if (isfile(join(CSV_PATH_INPUT, f)) and
                    f.startswith("comment") and f.endswith("csv") and
                    f.find(channel) >= 0 and not is_done(situations, f)):
                self.read_comment_by_block(f)
                write_situation(situations, f)
                do_something = True
        return do_something

    def read_comment(self, file_name):
        with open(CSV_PATH_INPUT + file_name) as file_comment:
            with open(CSV_PATH_OUTPUT + file_name, "w") as file_out:
                print(str(datetime.now()) + " read_comment: " + file_name)
                csv_reader = csv.reader(file_comment, delimiter=',')
                csv_writer = csv.writer(file_out, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                next(csv_reader, None)
                csv_writer.writerow(["videoId", "commentId", "authorId", "authorName", "text", "replies", "likes", "publishedAt", "sentiment", "weight"])
                ok = 0
                nok = 0
                n = 0
                start = datetime.now()
                for row in csv_reader:
                    n += 1
                    sentiment, weight = self.predictor.eval(row[4])
                    if weight > WEIGHT_LIMIT:
                        ok += 1
                    else:
                        sentiment = "UNKNOWN"
                        nok += 1
                    row.append(sentiment)
                    row.append(weight)
                    csv_writer.writerow(row)
                    if n % 10000 == 0:
                        elapsed = datetime.now() - start
                        print(" Comment processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok) + " Elapsed:" + str(elapsed)[:-7])
                    elif n % 1000 == 0:
                        print(".", end='', flush=True)
                elapsed = datetime.now() - start
                print(" Comment processed:" + str(n) + " Ok:" + str(ok) + " Not ok:" + str(nok) + " Elapsed:" + str(elapsed)[:-7])

    def write_block(self, csv_writer, rows, texts):
        y_prob = self.predictor.eval_block(texts)
        for p, prediction in enumerate(y_prob):
            pred = y_prob.argmax(axis=-1)[p]
            w = float(prediction[pred])
            if w < WEIGHT_LIMIT:
                sentiment = "UNKNOWN"
            else:
                sentiment = CLASSES[pred]
            rows[p].append(sentiment)
            rows[p].append(w)
        csv_writer.writerows(rows)
        print("*", end='', flush=True)

    def read_comment_by_block(self, file_name):
        with open(CSV_PATH_INPUT + file_name) as file_comment:
            with open(CSV_PATH_OUTPUT + file_name, "w") as file_out:
                print(str(datetime.now()) + " read_comment: " + file_name)
                csv_reader = csv.reader(file_comment, delimiter=',')
                csv_writer = csv.writer(file_out, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                next(csv_reader, None)
                csv_writer.writerow(["videoId", "commentId", "authorId", "authorName", "text", "replies", "likes", "publishedAt", "sentiment", "weight"])
                n = 0
                start = datetime.now()
                rows = []
                texts = []
                for row in csv_reader:
                    rows.append(row)
                    texts.append(row[4])
                    n += 1
                    if len(rows) >= BLOCK_SIZE:
                        self.write_block(csv_writer, rows, texts)
                        rows = []
                        texts = []
                    if n % 10000 == 0:
                        elapsed = datetime.now() - start
                        print(" Comment processed:" + str(n) + " Elapsed:" + str(elapsed)[:-7])
                    elif n % 1000 == 0:
                        print(".", end='', flush=True)
                if len(rows) > 0:
                    self.write_block(csv_writer, rows, texts)
                elapsed = datetime.now() - start
                print(" Comment processed:" + str(n) + " Elapsed:" + str(elapsed)[:-7])


main = Main()
sleeping = False
while True:
    if main.run(""):
        sleeping = False
    else:
        if sleeping:
            print(".", end='', flush=True)
        else:
            print("Sleeping .")
            sleeping = True
        time.sleep(5*60)

