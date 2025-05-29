import spacy
from spacy.training.example import Example
from spacy.util import minibatch
import random

TRAIN_DATA = [
    ("Старший администратор ветеринарной клиники", {"entities": [(0, 42, "TITLE")]}),
    ("Москва, Малая Пироговская улица, 25", {"entities": [(0, 34, "LOCATION")]}),
    ("Зарплата 75 000 — 90 000 ₽", {"entities": [(9, 28, "SALARY")]}),
]

nlp = spacy.load("ru_core_news_lg")

if "ner" not in nlp.pipe_names:
    ner = nlp.add_pipe("ner", last=True)
else:
    ner = nlp.get_pipe("ner")

for _, annotations in TRAIN_DATA:
    for ent in annotations.get("entities"):
        ner.add_label(ent[2])

other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]
with nlp.disable_pipes(*other_pipes):
    optimizer = nlp.initialize()
    for i in range(30):
        random.shuffle(TRAIN_DATA)
        batches = minibatch(TRAIN_DATA, size=2)
        for batch in batches:
            for text, annotations in batch:
                example = Example.from_dict(nlp.make_doc(text), annotations)
                nlp.update([example], sgd=optimizer, drop=0.4)

nlp.to_disk("vacancy_model")
