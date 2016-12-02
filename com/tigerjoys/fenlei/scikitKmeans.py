# encoding:utf-8
import sys
import jiebafenci
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.cluster import KMeans


def generateCorpus():
    path = jiebafenci.pre_fenci(sys.argv[1])
    corpus = []
    for line in open(path).readlines():
        corpus.append(line)
    vectorizer = CountVectorizer()
    transformer = TfidfTransformer()
    tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))
    km = KMeans(n_clusters=int(sys.argv[2]))
    km_result = km.fit(tfidf)
    # print km_result
    # print km.cluster_centers_
    # print km.inertia_
    # print km.labels_
    return km.labels_


def getResult():
    n_clusters = int(sys.argv[2])
    nameIndex = generateCorpus()
    fileHandler = []
    for i in range(n_clusters):
        fileHandler.append(open("fenleijieguo-" + str(i), mode='wa+'))
    lineNum = 0
    for line in open(sys.argv[1]).readlines():
        fileHandler[nameIndex[lineNum]].write(line)
        lineNum += 1


if __name__ == '__main__':
    getResult()
