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
import pymongo


def process_text(s: str):
    out_s = []
    for word in s:
        if word not in string.punctuation:
            out_s.append(word)
    return out_s


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


def remove_duplicate(threshold=0.8):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper']

    documents = list(collection.find())
    titles = [process_text(document['title']) for document in documents]

    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(titles)
    cosine_sim_matrix = cosine_similarity(tfidf_matrix, tfidf_matrix)

    duplicated_document_ids = []
    for i in range(len(titles)):
        for j in range(i + 1, len(titles)):
            if cosine_sim_matrix[i, j] > threshold:
                date_i = documents[i]['published_date']
                date_j = documents[j]['published_date']
                time_diff = abs((date_i - date_j).total_seconds())

                # if time difference is < 3 hours -> duplicated
                limit = 3 * 60 * 60
                if time_diff < limit:
                    duplicated_document_ids.append(documents[j]['_id'])

    result = collection.delete_many({'_id': {'$in': duplicated_document_ids}})
    print(f'Deleted {len(duplicated_document_ids)} duplicated document')


def delete_duplicate_link():
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


if __name__ == '__main__':
    pass