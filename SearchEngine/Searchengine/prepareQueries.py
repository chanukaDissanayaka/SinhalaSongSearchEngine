from nltk import ngrams
import json
import re


excluded = []
with open('./Searchengine/data/excluded.json', encoding='utf-8') as fp:
    excluded = json.load(fp)


def tokanize_sinhala(text):
    punctuations = '''!()-[]{};:'"\,<>./?@#$%^&_~'''

    new_text = text

    for i in text:
        if i in punctuations:
            new_text = new_text.replace(i, " ")

    tokens = new_text.split(' ')
    return tokens


def addToResultTokens(dict, field, value):
    if value != "":
        if not value.isdigit():
            if value not in dict[field]:
                dict[field].append(value)


def classifyQuery(tokens):
    result = {
        'raw': [],
        'title': [],
        'artist': [],
        'writer': [],
        'lyrics': [],
        'SortbyVisits': False,
        'count': 0,
        'genre': [],
        'stopwords': [],
        'stopword_excluded': [],
        'artist_weight': 1,
        'artist_weight': 1,
        'writer_weight': 1,
        'subject_weight': 1,
    }
    new_tokens = []

    genres = ['Old Pops', 'New Pop', 'Request', 'Classics',
              'Golden Oldies', 'Golden Pop', 'Inspirational', 'Duets', 'Movie Songs', 'Calypso', 'Current Songs',
              'Group Songs', 'duet', 'reques', 'Kids Songs', 'Movie', 'My Picks']

    stopwords_artist = ['ගයන', 'ගැයු', 'ගායන',
                        'කියන', 'සින්දු', 'ගීත', 'ගී', 'ගැයූ']
    stopwords_subject = ["ගැන", "පිලිබඳ", "පිලිබද", 'සින්දු', 'ගීත', 'ගී']
    stopwords_compare = ["හොඳම", "හොදම", "ජනප්‍රියම", "ජනප්‍රිය",
                         "ප්‍රසිද්ධ", "ප්‍රසිද්ධම", "ප්‍රසිද්ද", "ප්‍රසිද්දම"]
    stopwords_writer = ['ලියූ', 'රචනා', 'රචනය', 'සින්දු', 'ගීත', 'ගී', ]

    stopwords_all = (stopwords_artist + stopwords_subject +
                     stopwords_compare + stopwords_writer + genres)
    listIndex = 0
    removed = []

    for token in tokens:
        if (not re.search('[a-zA-Z]', token)):
            
            # song by title
            
            if(token not in stopwords_all):
                addToResultTokens(result, "title", token)

            # song of an artist

            isArtist = False

            if ('ගේ' in token[-2:]):

                if token not in excluded:
                    isArtist = True
                    for pre_word in tokens[:listIndex]:
                        if(pre_word not in stopwords_all):
                            addToResultTokens(result, "artist", pre_word)
                            addToResultTokens(
                                result, "stopword_excluded", pre_word)

                    addToResultTokens(result, "raw", token)
                    addToResultTokens(result, "artist", token[:-2])
                    addToResultTokens(result, "stopword_excluded", token[:-2])
                    addToResultTokens(result, "stopwords", token[-2:])
                    result['artist_weight'] = 2
                    removed.append(token)

                else:
                    for pre_word in tokens[:listIndex]:
                        if(pre_word not in stopwords_all):
                            addToResultTokens(result, "artist", pre_word)
                            addToResultTokens(
                                result, "stopword_excluded", pre_word)

                    addToResultTokens(result, "raw", token)
                    addToResultTokens(result, "artist", token)
                    addToResultTokens(result, "stopword_excluded", token)
                    result['artist_weight'] = 2

            if(token in stopwords_artist):
                addToResultTokens(result, "stopwords", token)
                for other_word in tokens:
                    if(other_word not in stopwords_all):
                        if(other_word not in removed):
                            if(other_word != token):
                                addToResultTokens(result, "artist", other_word)

    

            else:
                addToResultTokens(result, "raw", token)
                new_tokens.append({"token": token, "type": "raw"})

            # songs about a subject
            if (token in stopwords_subject):
                addToResultTokens(result, "stopwords", token)
                for pre_word in tokens:
                    if(pre_word not in stopwords_all):
                        addToResultTokens(result, "lyrics", pre_word)

            else:
                addToResultTokens(result, "raw", token)
                if token not in stopwords_all:
                    addToResultTokens(result, "lyrics", token)

            # songs by writer
            if ('ගේ' in token[-2:]):
                addToResultTokens(result, "raw", token)
                addToResultTokens(result, "writer", token[:-2])
                addToResultTokens(result, "stopword_excluded", token[:-2])
                addToResultTokens(result, "stopwords", token[-2:])
                result['writer_weight'] = 2
                removed.append(token)

            if(token in stopwords_writer):
                addToResultTokens(result, "stopwords", token)
                for other_word in tokens:
                    if(other_word not in stopwords_all):
                        if(other_word not in removed):
                            if(other_word != token):
                                addToResultTokens(result, "writer", other_word)

            # best songs by comparing
            if (token in stopwords_compare):
                result['SortbyVisits'] = True
                addToResultTokens(result, "stopwords", token)

            if (token.isdigit()):
                result['count'] = int(token)
                result['SortbyVisits'] = True

            addToResultTokens(result, "raw", token)  # new

            # song by lyrics

            # setweights
            if (token in stopwords_artist):
                result['artist_weight'] = 2
            if (token in stopwords_writer):
                result['writer_weight'] = 2
            if (token in stopwords_subject):
                result['subject_weight'] = 2

        else:
            if(isGenreToken(token)):
                addToResultTokens(result, "genre", token)
                addToResultTokens(result, "stopwords", token)
            addToResultTokens(result, "raw", token)  # new

        listIndex = listIndex + 1

    for key in ['raw', 'title', 'artist', 'writer', 'lyrics', 'stopwords', 'genre']:
        for word in result[key]:
            if word not in result['stopwords']:
                addToResultTokens(result, "stopword_excluded", word)

        for word in result['genre']:
            addToResultTokens(result, "stopword_excluded", word)

    return result


def isGenreToken(token):
    genres = ['Old Pops', 'New Pop', 'Request', 'Classics',
              'Golden Oldies', 'Golden Pop', 'Inspirational', 'Duets', 'Movie Songs', 'Calypso', 'Current Songs',
              'Group Songs', 'duet', 'reques', 'Kids Songs', 'Movie', 'My Picks']

    for i in genres:
        if token in i:
            return True
    else:
        return False


def getTokens(query):
    tokens = tokanize_sinhala(query)
    new_tokens = classifyQuery(tokens)
    return new_tokens
