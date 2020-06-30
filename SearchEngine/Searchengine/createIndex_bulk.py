import json
from elasticsearch import Elasticsearch, helpers

with open('./data/song_lyrics_final.json', encoding='utf-8') as fp:
    data = json.load(fp)

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


request_body = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "max_ngram_diff": "50"
        },
        "analysis": {

            "char_filter": {

                "punctuation_remove_filter": {
                    "type": "mapping",
                    "mappings": [
                            ". => \\u0020",
                            ", => \\u0020",
                            "| => \\u0020"
                    ]
                }
            },

            "filter": {

                "ngram_artist_filter": {
                    "type": "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 10,
                    "token_chars": ["letter"]
                },

                "ngram_genre_filter": {
                    "type": "edge_ngram",
                    "min_gram": 3,
                    "max_gram": 20
                },

                "ngram_lyric_filter": {
                    "type": "edge_ngram",
                    "min_gram": 3,
                    "max_gram": 20
                },

                "ngram_title_filter": {
                    "type": "edge_ngram",
                    "min_gram": 5,
                    "max_gram": 20
                }
            },

            "analyzer": {

                "artist_analyzer": {
                    "type": "custom",
                    "char_filter": ["punctuation_remove_filter"],
                    "tokenizer": "whitespace",
                    "filter": ["ngram_artist_filter"],

                },

                "title_analyzer_ngram": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "char_filter": ["punctuation_remove_filter"],
                    "filter": ["ngram_title_filter"],

                },

                "lyric_analyzer": {
                    "type": "custom",
                    "char_filter": ["punctuation_remove_filter"],
                    "tokenizer": "whitespace",
                    "filter": [],

                },

                "lyric_analyzer_ngram": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "char_filter": ["punctuation_remove_filter"],
                    "filter": ["ngram_lyric_filter"],

                },

                "genre_analyzer": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase", "ngram_genre_filter"],
                    "char_filter": ["punctuation_remove_filter"]
                },

                "whitespace_analyzer": {
                    "tokenizer": "whitespace"
                }
            },

        }
    },
    "mappings": {
        "properties": {
            "title_sin": {
                "type": "text",
                "analyzer": "whitespace_analyzer",
                # "search_analyzer": "title_analyzer_ngram"
            },
            "title_en": {
                "type": "text"
            },
            "artist": {
                "type": "text",
            },
            "artist_sin": {
                "type": "text",
                "analyzer": "artist_analyzer",
                "search_analyzer": "artist_analyzer",
            },
            "genre": {
                "type": "text",
                "analyzer": "genre_analyzer",
                "search_analyzer": "genre_analyzer",
            },
            "writer": {
                "type": "text",
            },
            "writer_sin": {
                "type": "text",
                "analyzer": "whitespace_analyzer"
            },
            "music": {
                "type": "text",
                "analyzer": "whitespace_analyzer"
            },
            "music_sin": {
                "type": "text",
                "analyzer": "whitespace_analyzer"
            },
            "song": {
                "type": "text",
                "analyzer": "lyric_analyzer_ngram",
                "search_analyzer": "lyric_analyzer"
            },
            "total_visits": {
                "type": "integer"
            }

        }
    }
}


es.indices.delete(index='testsong_4')
es.indices.create(index='testsong_4', body=request_body)


a = es.indices.get_alias().keys()
print(a)


helpers.bulk(es, data, index='testsong_4',
             doc_type='_doc', request_timeout=200)
