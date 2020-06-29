from elasticsearch import Elasticsearch as es
import elasticsearch
from elasticsearch.helpers import scan
import json
import Searchengine.prepareQueries as prepareQueries

es = elasticsearch.Elasticsearch('http://localhost:9200')

score_factor = 0.7


def matchParams(tokens):

    genres = tokens['genre']

    # artist

    if(len(tokens['artist']) > 0):
        print("artist")

        weighted_artist = "artist_sin^"+str(tokens['artist_weight'])
        weighted_writer = "writer_sin^"+str(tokens['writer_weight'])
        weighted_subject = "song^"+str(tokens['subject_weight'])
        # genre is available
        if(len(genres) != 0):

            query = {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "genre": {
                                    "query": " ".join(genres)
                                }
                            }
                        },

                        {
                            "bool": {
                                "should": [
                                    {
                                        "multi_match": {
                                            "query":    ' '.join(tokens['stopword_excluded']),
                                            "fields": ["title_sin", weighted_artist, weighted_writer, "music_sin", weighted_subject, "genre^2"]
                                        }
                                    },

                                    {
                                        "match_phrase": {
                                            "song": ' '.join(tokens['lyrics'])
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "artist_sin": ' '.join(tokens['artist'])
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "title_sin": ' '.join(tokens['title'])
                                        }
                                    }

                                ]
                            }
                        }
                    ]
                }
            }

        # no genre
        else:
            query = {

                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query":    ' '.join(tokens['stopword_excluded']),
                                            "fields": ["title_sin", weighted_artist, weighted_writer, "music_sin", weighted_subject, "genre^2"]
                            }
                        },

                        {
                            "match_phrase": {
                                "song": ' '.join(tokens['lyrics'])
                            }
                        },
                        {
                            "match_phrase": {
                                "artist_sin": ' '.join(tokens['artist'])
                            }
                        },
                        {
                            "match_phrase": {
                                "title_sin": ' '.join(tokens['title'])
                            }
                        }

                    ]
                }
            }

    # subject

    elif(tokens['subject_weight'] > 1):
        print("subject")

        weighted_artist = "artist_sin^"+str(tokens['artist_weight'])
        weighted_writer = "writer_sin^"+str(tokens['writer_weight'])
        weighted_subject = "song^"+str(tokens['subject_weight']+2)
        genres = tokens['genre']

        # genre available
        if(len(genres) != 0):
            query = {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "genre": {
                                    "query": " ".join(genres)
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "multi_match": {
                                            "query":    ' '.join(tokens['stopword_excluded']),
                                            "fields": ["title_sin", weighted_artist, weighted_writer, "music_sin", weighted_subject, "genre^2"]
                                        }
                                    },

                                    {
                                        "match_phrase": {
                                            "song": ' '.join(tokens['lyrics'])
                                        }
                                    },

                                ]
                            }
                        }
                    ]
                }
            }
        # no genre
        else:
            query = {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query":    ' '.join(tokens['stopword_excluded']),
                                "fields": ["title_sin", weighted_artist, weighted_writer, "music_sin", weighted_subject, "genre^2"]
                            }
                        },

                        {
                            "match_phrase": {
                                "song": ' '.join(tokens['lyrics'])
                            }
                        },

                    ]
                }
            }
    else:
        print("else")

        score_factor = 0.5
        # genre
        if(len(genres) != 0):
            query = {
                "bool": {
                    "must": [

                        {
                            "bool": {
                                "should": [
                                    {
                                        "multi_match": {
                                            "query":    ' '.join(tokens['stopword_excluded']),
                                            "fields": ["title_sin", "artist_sin", "writer_sin", "music_sin", "song^2", "genre"],
                                        }
                                    },
                                ]
                            }
                        },
                        {
                            "match": {
                                "genre": {
                                    "query": " ".join(genres)
                                }
                            }
                        },
                    ]
                }

            }
        # no genre
        else:
            query = {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query":    ' '.join(tokens['stopword_excluded']),
                                "fields": ["title_sin", "artist_sin", "writer_sin", "music_sin", "song^2", "genre"],
                            }
                        },

                        # {
                        #     "match": {
                        #         'song': ' '.join(tokens['stopword_excluded']),
                        #     },
                        # },
                        # {
                        #     "match": {
                        #         'title_sin': ' '.join(tokens['stopword_excluded']),
                        #     },
                        # },
                        # {
                        #     "match": {
                        #         'artist_sin': ' '.join(tokens['stopword_excluded']),
                        #     },
                        # },
                        # {
                        #     "match": {
                        #         'writer_sin': ' '.join(tokens['stopword_excluded']),
                        #     },
                        # },
                        # {
                        #     "match": {
                        #         'music_sin': ' '.join(tokens['stopword_excluded']),
                        #     },
                        # },
                        # {
                        #     "match": {
                        #         'genre': ' '.join(tokens['stopword_excluded']),
                        #     },
                        # }


                    ]
                }
            }

    if (len(tokens['stopword_excluded']) == 0):
        query = {
            "match_all": {}
        }

    return query


def createQueryParams(query):
    processed_tokens = prepareQueries.getTokens(query)
    writeToFile('test_tokens', processed_tokens)

    query_parms = matchParams(processed_tokens)

    sort_by_visit = processed_tokens["SortbyVisits"]
    result_count = processed_tokens["count"]

    if result_count == 0:
        result_count = 1000

    if (sort_by_visit):
        query_setting = {
            "query": {
                'track_scores': 'true',
                "sort": ["_score",
                         {"total_visits": "desc"}
                         ],
                'query': query_parms
            },
            "count": result_count
        }
    else:
        query_setting = {
            "query": {
                'track_scores': 'true',
                "sort": ["_score",
                         # {"total_visits": "desc"}
                         ],
                'query': query_parms
            },
            "count": result_count
        }

    return query_setting


def search(keywords):
    query_setting = createQueryParams(keywords)
    writeToFile('test_query', query_setting)

    res = es.search(index="testsong_4",
                    body=query_setting['query'], size=query_setting['count'])
    writeToFile('test_res', res)

    results = []
    for i in res['hits']['hits']:
        if (i["_score"] >= (res['hits']['max_score'] * score_factor)):
            results.append(i)

    writeToFile('test_results_fliterd', results)

    return results


def writeToFile(filename, data):
    with open('./test/'+filename+'.txt', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
