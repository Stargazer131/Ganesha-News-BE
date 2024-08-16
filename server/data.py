from time import time
from pymongo import MongoClient
import json
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime
import random
from underthesea import word_tokenize
import pickle
import numpy as np
from pymongo import MongoClient, UpdateOne
import unicodedata
from copy import deepcopy
from bson.objectid import ObjectId
from ann_search import load_stop_words, load_punctuations


def create_punctuations_string():
    punctuations = ''.join(
        chr(i) for i in range(0x110000)
        if unicodedata.category(chr(i)).startswith('P') or
        unicodedata.category(chr(i)).startswith('S') or
        unicodedata.category(chr(i)).startswith('N')
    )
    return punctuations

stop_words = load_stop_words()
punctuations = load_punctuations()
translator = str.maketrans('', '', punctuations)

def process_title(s: str):
    s = s.lower().translate(translator)
    tokens = word_tokenize(s)
    return ' '.join([token.replace(' ', '_') for token in tokens if token not in stop_words])


def remove_duplicate_title(threshold=0.8):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']
    b_collection = db['black_list']
    p_collection = db['processed_content']

    projection = {"published_date": 1, "link": 1, "web": 1, "_id": 0}
    documents = list(collection.find({}, projection))
    titles = [doc['title']
              for doc in p_collection.find({}, {"title": 1, "_id": 0})]

    vectorizer = TfidfVectorizer(lowercase=False)
    tfidf_matrix = vectorizer.fit_transform(titles)
    cosine_sim_matrix = cosine_similarity(tfidf_matrix, dense_output=False)

    rows, cols = cosine_sim_matrix.nonzero()
    values = cosine_sim_matrix.data
    filter_index = np.where(values >= threshold)[0]
    result = [(rows[i], cols[i]) for i in filter_index if rows[i] < cols[i]]

    duplicated_document_ids = set()
    black_list = []
    for i, j in result:
        date_i = documents[i]['published_date']
        date_j = documents[j]['published_date']
        time_diff = abs((date_i - date_j).total_seconds())

        # if time difference < 5 hours -> duplicated
        limit = 5 * 3600
        if time_diff < limit:
            obj_id = documents[j]['_id']
            if obj_id not in duplicated_document_ids:
                duplicated_document_ids.add(obj_id)
                black_list.append({
                    'link': documents[j]['link'],
                    'web': documents[j]['web']
                })

    result = collection.delete_many(
        {'_id': {'$in': list(duplicated_document_ids)}})
    print(f'Deleted {result.deleted_count} duplicated document')

    result = b_collection.insert_many(black_list)
    print(f'Added {len(result.inserted_ids)} black list document')


def backup_data(collection_name='newspaper_v2'):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    output_dir = f'data/Ganesha_News'
    os.makedirs(output_dir, exist_ok=True)

    collection = db[collection_name]
    data = list(collection.find())

    with open(os.path.join(output_dir, f'{collection_name}.json'), 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str, ensure_ascii=False)

    client.close()


def count_category_document():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']
    category_map = {}
    for doc in collection.find():
        category = doc['category']
        if category not in category_map:
            category_map[category] = 0
        category_map[category] += 1

    for key, value in category_map.items():
        print(f'Category {key} has {value} documents')


def bulk_update():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']

    # filter_query = {"link": {"$regex": "vietnamnet"}}
    # update_query = {"$set": {"web": "vietnamnet"}}

    # result = collection.update_many(filter_query, update_query)
    # print(f"Updated {result.modified_count} documents.")

    # Define the update operation
    bulk_updates = []
    index = 0
    for doc in collection.find({}, {"_id": 1}):
        bulk_updates.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"index": index}}
            )
        )
        index += 1

    result = collection.bulk_write(bulk_updates)
    print(f"Modified {result.modified_count} documents.")


def caculate_time(function: callable):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        function(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Execution time:", elapsed_time, "seconds")
    
    return wrapper


def test_remove_duplicated():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']

    # cursor = collection.find({}, {"id": 1})
    # db_ids = set([str(doc['_id']) for doc in cursor])

    black_list = []
    origin_list = []
    with open('data/Ganesha_News/newspaper_v2.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        for obj in data:
            temp = deepcopy(obj)
            temp['_id'] = ObjectId(temp['_id'])
            temp['published_date'] = datetime.strptime(
                temp['published_date'], "%Y-%m-%d %H:%M:%S")
            origin_list.append(temp)

    collection.insert_many(origin_list)

    # bcollection = db['black_list']
    # bcollection.delete_many({"link" : {"$in": black_list}})


def preprocess_titles():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']
    pcollection = db['processed_content']

    projection = {"title": 1, "_id": 0}
    titles = [doc['title'] for doc in collection.find({}, projection)]
    processed_titles = [{"title": process_title(title)} for title in titles]

    pcollection.insert_many(processed_titles)


if __name__ == '__main__':
    pass
