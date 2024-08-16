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


def get_title_and_descriptions():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']

    title_and_descriptions = []
    cursor = collection.find({}, {"title": 1, "description": 1, "_id": 0})
    for doc in cursor:
        title = doc['title']
        description = doc['description']
        if not title.endswith('.'):
            title += '.'
        title_and_descriptions.append(title + '\n' + description)

    return title_and_descriptions


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


def remove_duplicate_link():
    client = MongoClient("mongodb://localhost:27017/")
    db = client['Ganesha_News']
    collection = db['black_list']

    # Find duplicates based on the 'link' attribute
    pipeline = [
        {"$group": {"_id": "$link", "count": {"$sum": 1}, "docs": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}}
    ]

    duplicates = collection.aggregate(pipeline)

    # Remove duplicates, keeping only one document per link
    for duplicate in duplicates:
        # Keep the first document and remove the rest
        ids_to_keep = duplicate['docs'][1:]
        collection.delete_many({'_id': {'$in': ids_to_keep}})

    print("Duplicate documents removed and unique index created based on 'link' attribute.")


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


def delete_all_data():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']
    filter = {"link": {"$regex": "dantri"}}
    print(collection.delete_many(filter).deleted_count)


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


def delete_duplicated():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']
    doc_set = set()
    delete_id = []
    for doc in collection.find():
        link = doc['link']
        if link in doc_set:
            delete_id.append(doc['_id'])
        else:
            doc_set.add(link)

    filter = {"_id": {"$in": delete_id}}
    print(collection.delete_many(filter).deleted_count)


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


def count_duplicated(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        data = file.readlines()

    web_list = ['dantri', 'vnexpress', 'vtcnews', 'vietnamnet']
    web_map = {}

    for web in web_list:
        web_map[web] = {}
        for w in web_list:
            web_map[web][w] = 0

    for i in range(0, len(data) - 5, 6):
        link1, link2 = data[i], data[i + 1]
        for web in web_list:
            if web in link1:
                link1 = web
            if web in link2:
                link2 = web

        if link1 == link2:
            web_map[link1][link2] += 1
        else:
            web_map[link1][link2] += 1
            web_map[link2][link1] += 1

    for web in web_list:
        for w in web_list:
            print(f'DUPLICATED betwwen {web} and {w}: {web_map[web][w]}')


def paginated_result(page=1, page_size=20):
    pass


def caculate_time(function: callable):
    start_time = time()
    function()
    end_time = time()
    elapsed_time = end_time - start_time
    print("Execution time:", elapsed_time, "seconds")


def aggerate_vs_find():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']
    web = 'vtcnews'

    # pipeline = [
    #     {"$match": {"web": web}},
    #     {"$project": {"link": 1, "_id": 0}}
    # ]

    # set(doc['link'] for doc in collection.aggregate(pipeline))

    set(doc['link'] for doc in collection.find(
        {"web": web}, {"link": 1, "_id": 0}))


def paginate_result(page=10, page_size=20):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']

    filter = {"category": 'thoi-su'}
    projection = {"title": 1, "description": 1, "thumbnail": 1}
    skip_doc = (page - 1) * page_size
    cursor = collection.find(
        filter, projection, limit=page_size, skip=skip_doc)
    print(list(cursor))


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


def shuffle_database():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']

    origin_list = []
    with open('data/Ganesha_News/newspaper_v2.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        for obj in data:
            temp = deepcopy(obj)
            temp['_id'] = ObjectId(temp['_id'])
            temp['published_date'] = datetime.strptime(
                temp['published_date'], "%Y-%m-%d %H:%M:%S")
            origin_list.append(temp)

    random.shuffle(origin_list)
    collection.insert_many(origin_list)


if __name__ == '__main__':
    pass
