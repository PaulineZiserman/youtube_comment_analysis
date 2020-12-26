import csv

import keras
import pickle
from keras.preprocessing.sequence import pad_sequences

MAX_NB_WORDS = 40000 # max no. of words for tokenizer
MAX_SEQUENCE_LENGTH = 30 # max length of text (words) including padding
VALIDATION_SPLIT = 0.2
EMBEDDING_DIM = 200 # embedding dimensions for word vectors (word2vec/GloVe)

model = keras.models.load_model("checkpoint-0.917.h5")

text = []
check1 = []
with open('fzdata/check3.csv', "r") as f:
    csv_reader = csv.reader(f, delimiter=',')
    next(csv_reader, None)
    for line in csv_reader:
        text.append(line[0])
        check1.append(line)

with open('tokenizer.pickle', 'rb') as handle:
    tokenizer = pickle.load(handle)

classes = ["neutral", "happy", "sad", "hate","anger"]

sequences_test = tokenizer.texts_to_sequences(text)
data_int_t = pad_sequences(sequences_test, padding='pre', maxlen=(MAX_SEQUENCE_LENGTH-5))
data_test = pad_sequences(data_int_t, padding='post', maxlen=(MAX_SEQUENCE_LENGTH))
y_prob = model.predict(data_test)

with open('fzdata/check3_result.csv', "w") as f:
    csv_writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["text", "expected", "prediction", "weight", "neutral", "happy", "sad", "hate","anger"])
    for n, prediction in enumerate(y_prob):
        pred = y_prob.argmax(axis=-1)[n]
        w = float(prediction[pred])
        csv_writer.writerow([
            check1[n][0],
            check1[n][1],
            classes[pred],
            str(w),
            prediction[0],
            prediction[1],
            prediction[2],
            prediction[3],
            prediction[4],
        ])

 #   print(text[n],"\nPrediction:",classes[pred] + ":" + str(w),"\n")