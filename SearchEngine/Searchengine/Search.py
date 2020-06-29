from elasticsearch import Elasticsearch as es
import elasticsearch
from elasticsearch.helpers import scan
import json
import Searchengine.prepareQueries as prepareQueries

es = elasticsearch.Elasticsearch('http://localhost:9200')


def matchParams(tokens):

    if(len(tokens['artist']) > 0):

        weighted_artist = "artist_sin^"+str(tokens['artist_weight'])
        weighted_writer = "writer_sin^"+str(tokens['writer_weight'])
        weighted_subject = "song^"+str(tokens['subject_weight'])
        genres = tokens['genre']

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

    elif(tokens['subject_weight'] > 1):

        weighted_artist = "artist_sin^"+str(tokens['artist_weight'])
        weighted_writer = "writer_sin^"+str(tokens['writer_weight'])
        weighted_subject = "song^"+str(tokens['subject_weight']+2)
        genres = tokens['genre']

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
        query = {
            "function_score": {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "multi_match": {
                                    "query":    ' '.join(tokens['stopword_excluded']),
                                    "fields": ["title_sin", "artist_sin", "writer_sin", "music_sin", "song", "genre"],
                                }
                            },
                        ]
                    }
                },

            }
        }

    if (len(tokens['stopword_excluded']) == 0):
        query = {
            "match_all": {}
        }

    return query


def createQueryParams(query):
    processed_tokens = prepareQueries.getTokens(query)
    with open('./test/test_tokens.txt', 'w', encoding='utf-8') as f:
        json.dump(processed_tokens, f, ensure_ascii=False, indent=2)

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
    with open('./test/test_query.txt', 'w', encoding='utf-8') as f:
        json.dump(query_setting, f, ensure_ascii=False, indent=2)

    res = es.search(index="testsong_4",
                    body=query_setting['query'], size=query_setting['count'])
    with open('./test/test_res.txt', 'w', encoding='utf-8') as f:
        json.dump(res, f, ensure_ascii=False, indent=2)

    results = []
    for i in res['hits']['hits']:
        if (i["_score"] >= (res['hits']['max_score'] * 0.7)):
            results.append(i)
    with open('./test/test_results_fliterd.txt', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return results

