import pandas as pd

from tiny_distributed_db.Cluster import Cluster

"""
In this experiment I want to show a simple presentation of this application.
"""

NUMBER_OF_COUNTRY = 5
country = pd.DataFrame({
    'id': list(range(NUMBER_OF_COUNTRY)),
    'country_name': [
        "JAPAN",
        "FRANCE",
        "SPAIN",
        "ITALIA",
        "NEW-ZEALAND"
    ]
})

NUMBER_OF_CITY = 17
city = pd.DataFrame({
    'id': list(range(NUMBER_OF_CITY)),
    'city_name': [
        "CLERMONT-FERRAND",
        "STRASBOURG",
        "LANNION",
        "TOULOUSE",
        "LILLE",

        "TOKYO",
        "OSAKA",
        "KOBE",

        "VENISE",
        "ROME",
        "BOLOGNE",
        "TURIN",

        "VALENCE",
        "TURIN",
        "BILBAO",

        "AUCKLAND",
        "QUEENSTOWN"
    ],
    'fk': [1, 1, 1, 1, 1, 0, 0, 0, 3, 3, 3, 3, 2, 2, 2, 4, 4]
})


cluster = Cluster(3)
cluster.insert("country", country)

cluster.insert("city", city)
print(cluster)

df, w = cluster.broadcast_join("country", "id", "city", "fk")
print(w)
print(len(df.index))
print(df)
print()

df, w = cluster.shuffle_join("country", "id", "city", "fk")
print(w)
print(len(df.index))
print(df)
print()
