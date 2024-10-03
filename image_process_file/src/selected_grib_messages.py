MS_2_KNTS = 1.94384
K_2_C = 273.15

GRIB_MSG = {
    "Pressure reduced to MSL": {"sn": "prmsl", "level_type": "meanSea", "level": 0},
    "Wind speed (gust)": {
        "sn": "wg",
        "level_type": "surface",
        "level": 0,
        "convert": lambda x: x * MS_2_KNTS,
    },
    "2 metre temperature": {
        "sn": "t2",
        "level_type": "heightAboveGround",
        "level": 2,
        "convert": lambda x: x - K_2_C,
    },
    "2 metre relative humidity": {
        "sn": "hum2",
        "level_type": "heightAboveGround",
        "level": 2,
    },
    "Apparent temperature": {
        "sn": "at2",
        "level_type": "heightAboveGround",
        "level": 2,
        "convert": lambda x: x - K_2_C,
    },
    "10 metre U wind component": {
        "sn": "u10",
        "level_type": "heightAboveGround",
        "level": 10,
        "convert": lambda x: x * MS_2_KNTS,
    },
    "10 metre V wind component": {
        "sn": "v10",
        "level_type": "heightAboveGround",
        "level": 10,
        "convert": lambda x: x * MS_2_KNTS,
    },
    "Sunshine Duration": {"sn": "sdur", "level_type": "surface", "level": 0},
    "Precipitation rate": {
        "sn": "pra",
        "level_type": "surface",
        "level": 0,
        "convert": lambda x: x * 1,
    },
    "Precipitable water": {
        "sn": "pwa",
        "level_type": "atmosphereSingleLayer",
        "level": 0,
        "convert": lambda x: x * 1,
    },
}
