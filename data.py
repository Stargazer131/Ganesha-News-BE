from bs4 import BeautifulSoup
from pymongo import MongoClient
import json
import os
import string
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import copy
from datetime import timedelta, datetime
import random
from underthesea import word_tokenize
import pickle
import joblib
import numpy as np
from pymongo import MongoClient, UpdateOne


with open('data/stop_words.pkl', 'rb') as f:
    stop_words = pickle.load(f)

extra_punctuations = '‘' + '’' + '”' + '“' + '…'
translator = str.maketrans('', '', string.punctuation + extra_punctuations + string.digits)


def process_text(s: str):
    s = s.lower()
    s = s.translate(translator)
    tokens = word_tokenize(s)
    return [token.replace(' ', '_') for token in tokens if token not in stop_words]


# TODO: Change it
def create_preprocessed_database():
    client = MongoClient('mongodb://localhost:27017/')
    client.drop_database(f'preprocessed_newspaper')

    old_db = client['newspaper']
    new_db = client['preprocessed_newspaper']

    translator = str.maketrans('', '', string.punctuation + '‘'+'’')
    for collection_name in old_db.list_collection_names():
        old_collection = old_db[collection_name]
        documents = []

        for document in old_collection.find():
            title = document['title'].translate(translator)
            new_document = copy.deepcopy(document)
            new_document['title'] = title
            documents.append(new_document)

        new_collection = new_db[collection_name]
        new_collection.insert_many(documents)


def get_titles():
    """Retrieves a list of titles from a MongoDB collection.

    Args:
        collection: A PyMongo collection object.

    Returns:
        A list of titles.
    """
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']

    pipeline = [
        {"$project": {"title": 1, "_id": 0}}
    ]
    titles = [doc['title'] for doc in collection.aggregate(pipeline)]
    return titles


def remove_duplicate_title(threshold=0.8):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']

    documents = list(collection.find())
    # titles = get_titles()
    # vectorizer = joblib.load('data/tfidf_vectorizer.pkl')
    tfidf_matrix = joblib.load('data/tfidf_matrix.pkl')
    cosine_sim_matrix = cosine_similarity(tfidf_matrix, dense_output=False)

    rows, cols = cosine_sim_matrix.nonzero()
    values = cosine_sim_matrix.data
    filter_index = np.where(values >= threshold)[0]
    result = [(rows[i], cols[i], values[i]) for i in filter_index if rows[i] != cols[i]]

    duplicated_document_ids = []
    duplicated_list = []
    duplicated_list_time = []
    for i, j, similarity in result:
        date_i = documents[i]['published_date']
        date_j = documents[j]['published_date']
        time_diff = abs((date_i - date_j).total_seconds())

        # if time difference is < 5 hours -> duplicated
        limit = 5 * 3600
        if time_diff < limit:
            duplicated_list_time.append(f"Link: {documents[i]['link']}\nLink: {documents[j]['link']}\n")
            duplicated_list_time.append(f"Title: {documents[i]['title']}\nTitle: {documents[j]['title']}\n")
            duplicated_list_time.append(f"Similarity: {similarity}\n\n")
        else:
            duplicated_list.append(f"Link: {documents[i]['link']}\nLink: {documents[j]['link']}\n")
            duplicated_list.append(f"Title: {documents[i]['title']}\nTitle: {documents[j]['title']}\n")
            duplicated_list.append(f"Similarity: {similarity}\n\n")

            # duplicated_document_ids.append(documents[j]['_id'])
    
    with open('data/duplicated.txt', 'w', encoding='utf-8') as file:
        file.writelines(duplicated_list)

    with open('data/duplicated_time.txt', 'w', encoding='utf-8') as file:
        file.writelines(duplicated_list_time) 

    # result = collection.delete_many({'_id': {'$in': duplicated_document_ids}})
    # print(f'Deleted {len(duplicated_document_ids)} duplicated document')


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
    data = {
        'articles': list(collection.find())[:20]
    }

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
    for doc in collection.find({"web": "dantri"}):
        updated_description = str(doc['description']).strip().removeprefix('(Dân trí)')
        updated_description = updated_description.removeprefix(' - ')
        
        # Prepare the bulk update operation
        bulk_updates.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"description": updated_description}}
            )
        )

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

    for i in range(0, len(data) - 4, 5):
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


if __name__ == '__main__':
    bulk_update()
