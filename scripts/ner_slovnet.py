#!/usr/bin/env python
import typer
from navec import Navec
from slovnet import NER
from ipymarkup import show_dep_ascii_markup as show_markup


def get_ner():
    navec = Navec.load('navec_news_v1_1B_250K_300d_100q.tar')
    ner = NER.load('slovnet_ner_news_v1.tar')
    ner.navec(navec)
    return ner

def run(filename):
    f = open(filename, 'r', encoding='utf8')
    data = f.read()
    f.close()
    ner = get_ner()
    markup = ner(data)
    for span in markup.spans:  
        print(data[span.start:span.stop], span.type)
#    show_markup(markup.text, markup.spans)    
    print(markup)



if __name__ == "__main__":
    typer.run(run)