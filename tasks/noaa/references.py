import pandas as pd

DATASETS = {
    'countries': {
        'filename': 'country_codes.txt',
        'args': dict(
            colspecs=[(0, 2), (3, 50)],
            header=None,
            names=["COUNTRY_CODE", "COUNTRY"]
        ),
    },
    'stations': {
        'filename': 'stations_metadata.txt',
        'args': dict(
            colspecs=[(0, 11), (12, 20), (21, 30), (31, 37), (38, 40), (41, 71), (72, 75), (76, 79), (80, 85)],
            header=None,
            names=["ID", "LATITUDE", "LONGITUDE", "ELEVATION", "STATE", "NAME", "GSN FLAG", "HCN/CRN FLAG", "WMO ID"]
        ),
    },
    'daily_report': {
        'args': dict(
            colspecs=[
                (0, 11), (11, 15), (15, 17), (17, 21), (21, 26), (26, 27), (27, 28), (28, 29), (29, 34),
                (34, 35), (35, 36), (36, 37), (37, 42), (42, 43), (43, 44), (44, 45), (45, 50), (50, 51),
                (51, 52), (52, 53), (53, 58), (58, 59), (59, 60), (60, 61), (61, 66), (66, 67), (67, 68),
                (68, 69), (69, 74), (74, 75), (75, 76), (76, 77), (77, 82), (82, 83), (83, 84), (84, 85),
                (85, 90), (90, 91), (91, 92), (92, 93), (93, 98), (98, 99), (99, 100), (100, 101),
                (101, 106), (106, 107), (107, 108), (108, 109), (109, 114), (114, 115), (115, 116),
                (116, 117), (117, 122), (122, 123), (123, 124), (124, 125), (125, 130), (130, 131),
                (131, 132), (132, 133), (133, 138), (138, 139), (139, 140), (140, 141), (141, 146),
                (146, 147), (147, 148), (148, 149), (149, 154), (154, 155), (155, 156), (156, 157),
                (157, 162), (162, 163), (163, 164), (164, 165), (165, 170), (170, 171), (171, 172),
                (172, 173), (173, 178), (178, 179), (179, 180), (180, 181), (181, 186), (186, 187),
                (187, 188), (188, 189), (189, 194), (194, 195), (195, 196), (196, 197), (197, 202),
                (202, 203), (203, 204), (204, 205), (205, 210), (210, 211), (211, 212), (212, 213),
                (213, 218), (218, 219), (219, 220), (220, 221), (221, 226), (226, 227), (227, 228),
                (228, 229), (229, 234), (234, 235), (235, 236), (236, 237), (237, 242), (242, 243),
                (243, 244), (244, 245), (245, 250), (250, 251), (251, 252), (252, 253), (253, 258),
                (258, 259), (259, 260), (260, 261), (261, 266), (266, 267), (267, 268), (268, 269)
            ],
            header=None,
            names=[
                'ID', 'YEAR', 'MONTH', 'ELEMENT', 'VALUE1', 'MFLAG1', 'QFLAG1', 'SFLAG1', 'VALUE2', 'MFLAG2',
                'QFLAG2', 'SFLAG2', 'VALUE3', 'MFLAG3', 'QFLAG3', 'SFLAG3', 'VALUE4', 'MFLAG4', 'QFLAG4', 'SFLAG4',
                'VALUE5', 'MFLAG5', 'QFLAG5', 'SFLAG5', 'VALUE6', 'MFLAG6', 'QFLAG6', 'SFLAG6', 'VALUE7', 'MFLAG7',
                'QFLAG7', 'SFLAG7', 'VALUE8', 'MFLAG8', 'QFLAG8', 'SFLAG8', 'VALUE9', 'MFLAG9', 'QFLAG9', 'SFLAG9',
                'VALUE10', 'MFLAG10', 'QFLAG10', 'SFLAG10', 'VALUE11', 'MFLAG11', 'QFLAG11', 'SFLAG11', 'VALUE12',
                'MFLAG12', 'QFLAG12', 'SFLAG12', 'VALUE13', 'MFLAG13', 'QFLAG13', 'SFLAG13', 'VALUE14', 'MFLAG14',
                'QFLAG14', 'SFLAG14', 'VALUE15', 'MFLAG15', 'QFLAG15', 'SFLAG15', 'VALUE16', 'MFLAG16', 'QFLAG16',
                'SFLAG16', 'VALUE17', 'MFLAG17', 'QFLAG17', 'SFLAG17', 'VALUE18', 'MFLAG18', 'QFLAG18', 'SFLAG18',
                'VALUE19', 'MFLAG19', 'QFLAG19', 'SFLAG19', 'VALUE20', 'MFLAG20', 'QFLAG20', 'SFLAG20', 'VALUE21',
                'MFLAG21', 'QFLAG21', 'SFLAG21', 'VALUE22', 'MFLAG22', 'QFLAG22', 'SFLAG22', 'VALUE23', 'MFLAG23',
                'QFLAG23', 'SFLAG23', 'VALUE24', 'MFLAG24', 'QFLAG24', 'SFLAG24', 'VALUE25', 'MFLAG25', 'QFLAG25',
                'SFLAG25', 'VALUE26', 'MFLAG26', 'QFLAG26', 'SFLAG26', 'VALUE27', 'MFLAG27', 'QFLAG27', 'SFLAG27',
                'VALUE28', 'MFLAG28', 'QFLAG28', 'SFLAG28', 'VALUE29', 'MFLAG29', 'QFLAG29', 'SFLAG29', 'VALUE30',
                'MFLAG30', 'QFLAG30', 'SFLAG30', 'VALUE31', 'MFLAG31', 'QFLAG31', 'SFLAG31'
            ]
        )
    }
}


def load_dataset(dataset_name):
    """Load a downloaded dataset as a pandas dataframe.

    Arguments:
        dataset_name(str):
            Name of the dataset, supported values are 'countries', 'stations' and
            the value of a station ID for the raw monthly report.

    Returns:
        pandas.DataFrame
    """

    dataset_params = DATASETS.get(dataset_name)

    if dataset_params is None:
        dataset_params = DATASETS.get('daily_report')
        dataset_params['filename'] = f"./all_daily/ghcnd_all/{dataset_name}.dly"

    return pd.read_fwf(dataset_params['filename'], **dataset_params['args'])


def generate_daily_reports_column_info():
    """This function was used to generate the colnames and colspecs of the daily reports."""
    colnames = ["ID", "YEAR", "MONTH", "ELEMENT"]
    colspecs = [(0, 11), (11, 15), (15, 17), (17, 21)]
    last = 21
    for day in range(1, 32):
        colnames.append(f"VALUE{day}")
        colspecs.append((last, last+5))
        last += 5

        colnames.append(f"MFLAG{day}")
        colspecs.append((last, last+1))
        last += 1

        colnames.append(f"QFLAG{day}")
        colspecs.append((last, last+1))
        last += 1

        colnames.append(f"SFLAG{day}")
        colspecs.append((last, last+1))
        last += 1

    return colspecs, colnames
