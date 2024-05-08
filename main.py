import pandas as pd

city = pd.DataFrame({
    'id': list(range(17)),
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
})

gb = city.groupby(['fk'])
print(gb[0])
