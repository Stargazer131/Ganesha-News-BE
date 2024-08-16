from pynndescent import NNDescent
import pickle
import numba
import numpy as np
import joblib


FLOAT32_EPS = np.finfo(np.float32).eps
FLOAT32_MAX = np.finfo(np.float32).max

@numba.njit(fastmath=True)
def combined_distance(x, y):
    # prepare
    dim = x.shape[0]
    norm_x = 0.0
    norm_y = 0.0
    l1_norm_x = 0.0
    l1_norm_y = 0.0
    
    for i in range(dim):
        l1_norm_x += x[i]
        l1_norm_y += y[i]
        norm_x += x[i] ** 2
        norm_y += y[i] ** 2

    # cosine
    if norm_x == 0.0 and norm_y == 0.0:
        result_cos = 0.0
    elif norm_x == 0.0 or norm_y == 0.0:
        result_cos = 1.0
    else:
        result_cos = 0.0
        for i in range(dim):
            result_cos += x[i] * y[i]
        result_cos = 1.0 - (result_cos / np.sqrt(norm_x * norm_y))
        
    # jensen shannon
    result_jen = 0.0
    l1_norm_x_jen = l1_norm_x + FLOAT32_EPS * dim
    l1_norm_y_jen = l1_norm_y + FLOAT32_EPS * dim

    pdf_x = (x + FLOAT32_EPS) / l1_norm_x_jen
    pdf_y = (y + FLOAT32_EPS) / l1_norm_y_jen
    m = 0.5 * (pdf_x + pdf_y)

    for i in range(dim):
        result_jen += 0.5 * (
            pdf_x[i] * np.log(pdf_x[i] / m[i]) + pdf_y[i] * np.log(pdf_y[i] / m[i])
        )
        
    # hellinger
    if l1_norm_x == 0 and l1_norm_y == 0:
        result_hel = 0.0
    elif l1_norm_x == 0 or l1_norm_y == 0:
        result_hel = 1.0
    else:
        result_hel = 0.0
        for i in range(dim):
            result_hel += np.sqrt(x[i] * y[i])
        result_hel = np.sqrt(1 - result_hel / np.sqrt(l1_norm_x * l1_norm_y))
        
    # jaccard
    if l1_norm_x == 0 and l1_norm_y == 0:
        result_jac = 0.0
    elif l1_norm_x == 0 or l1_norm_y == 0:
        result_jac = 1.0
    else:
        intersection = 0.0
        for i in range(dim):
            if x[i] <= y[i]:
                intersection += x[i]
            else:
                intersection += y[i]
        result_jac = 1 - intersection / (l1_norm_x + l1_norm_y) * 2
    
    # combined
    return (result_cos + result_jen + result_hel + result_jac) / 4


def load_nndescent() -> NNDescent:
    with open('data/nndescent.pkl', "rb") as f:
        return pickle.load(f)


def load_topic_distributions() -> list[np.ndarray]:
    return joblib.load('data/topic_distributions')


def load_stop_words() -> set[str]:
    with open('data/stop_words.pkl', 'rb') as file:
        return pickle.load(file)


def load_punctuations() -> str:
    with open('data/punctuations.pkl', 'rb') as file:
        return pickle.load(file)

