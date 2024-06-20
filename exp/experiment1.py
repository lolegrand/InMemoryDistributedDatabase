import pandas as pd

from tiny_distributed_db.Cluster import Cluster

"""
Show the correctness of the shuffle and broadcast join strategy.
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
    ],
    'joinType': ["shuffle", "broadcast"] + ["shuffle"] * 3
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
        "MADRID",
        "BILBAO",

        "AUCKLAND",
        "QUEENSTOWN"
    ],
    'fk': [1, 1, 1, 1, 1, 0, 0, 0, 3, 3, 3, 3, 2, 2, 2, 4, 4],
    'joinType': ["broadcast"] * 5 + ["shuffle"] * 12
})

print(city)
print(country)

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

df, w = cluster.flow_join("country", "id", "joinType", "city", "fk", "joinType")
print(w)
print(len(df.index))
print(df)
print()
