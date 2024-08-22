from time import time
from pymongo import MongoClient
import json
import os
from underthesea import sent_tokenize, word_tokenize
from pymongo import MongoClient, UpdateOne
import unicodedata
import pickle
from pynndescent import NNDescent


def caculate_time(func: callable):
    start_time = time()
    func()
    end_time = time()
    executed_time = end_time - start_time
    print(f'Executed time: {executed_time:.3f}s')


def load_nndescent() -> NNDescent:
    with open('data/nndescent.pkl', "rb") as f:
        return pickle.load(f)
    
    
def save_nndescent(nndescent: NNDescent):
    with open('data/nndescent.pkl', "wb") as f:
        pickle.dump(nndescent, f)


def load_processed_titles() -> list[str]:
    with open('data/processed_titles.pkl', "rb") as f:
        return pickle.load(f)
    
    
def save_processed_titles(processed_titles: list[str]):
    with open('data/processed_titles.pkl', "wb") as f:
        pickle.dump(processed_titles, f)


def load_stop_words():
    with open('data/vietnamese-stopwords.txt', 'r', encoding='utf-8') as file:
        data = file.readlines()
        return set([word.strip() for word in data])


def create_punctuations_string():
    punctuations = ''.join(
        chr(i) for i in range(0x110000)
        if unicodedata.category(chr(i)).startswith('P') or
        unicodedata.category(chr(i)).startswith('S') or
        unicodedata.category(chr(i)).startswith('N')
    )
    return punctuations


stop_words = load_stop_words()
translator = str.maketrans('', '', create_punctuations_string())


def process_title(s: str):
    s = s.lower().translate(translator)
    tokens = word_tokenize(s)
    return ' '.join([token.replace(' ', '_') for token in tokens if token not in stop_words])


def process_sentence(s: str):
    s = s.lower().translate(translator)
    tokens = word_tokenize(s)
    return [token.replace(' ', '_') for token in tokens if token not in stop_words]


def process_paragraph(text: str):
    res = []
    texts = sent_tokenize(text)
    for text in texts:
        res.extend(process_sentence(text))
    return res


def process_content(content: list):
    res = []
    for element in content:
        if isinstance(element, str) and not element.startswith('IMAGECONTENT'):
            res.extend(process_paragraph(element))
    return res


def get_titles(collection_name: str):
    with MongoClient('mongodb://localhost:27017/') as client:
        db = client['Ganesha_News']
        collection = db[collection_name]
        projection = {"published_date": 1, "link": 1, "web": 1, "title": 1}
        return list(collection.find({}, projection))
    
    
def get_content(collection_name: str):
    with MongoClient('mongodb://localhost:27017/') as client:
        db = client['Ganesha_News']
        collection = db[collection_name]
        projection = {"title": 1, "description": 1, "content": 1}
        return list(collection.find({}, projection))


def total_documents(collection_name: str):
    with MongoClient('mongodb://localhost:27017/') as client:
        db = client['Ganesha_News']
        collection = db[collection_name]
        return collection.count_documents({})
    

def is_collection_empty_or_not_exist(collection_name: str):
    with MongoClient('mongodb://localhost:27017/') as client:
        db = client['Ganesha_News']

        if collection_name not in db.list_collection_names():
            return True

        if db[collection_name].count_documents == 0:
            return True
        
        return False


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


def bulk_update_reset_index():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Ganesha_News']
    collection = db['newspaper_v2']

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


def get_category_list(collection_name: str):
    with MongoClient('mongodb://localhost:27017/') as client:
        db = client['Ganesha_News']
        collection = db[collection_name]
        projection = {"category": 1}
        return list(collection.find({}, projection))
    

def test_accuracy(top_n=10):
    nndescent = load_nndescent()
    top_recommendations = nndescent.neighbor_graph[0]
    data = get_category_list('newspaper_v2')

    correct_recommendation = 0
    for recommendations in top_recommendations:
        main_category = data[int(recommendations[0])]['category']
        
        for index in recommendations[1 : top_n + 1]:
            category = data[int(index)]['category']
            if category == main_category:
                correct_recommendation += 1
            
    print(f'Total correct recommendation: {correct_recommendation} / {len(top_recommendations) * top_n}')    
    print(f'Accuracy: {correct_recommendation / (len(top_recommendations) * float(top_n)) * 100 : .2f} %')


def caculate_time(function: callable):
    start_time = time()
    function()
    end_time = time()
    elapsed_time = end_time - start_time
    print("Execution time:", elapsed_time, "seconds")


if __name__ == '__main__':
    test_accuracy()
        
        