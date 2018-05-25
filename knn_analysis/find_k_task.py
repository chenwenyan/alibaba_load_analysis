from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def graph_data():
    # 读取用于聚类的数据，并创建数据表
    data = pd.read_csv('../dataset/batch_task.csv', header=None, iterator=True)

    loop = True
    chunkSize = 100000
    chunks = []
    while loop:
        try:
            chunk = data.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    print(df.count())

