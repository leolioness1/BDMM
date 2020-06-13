import pandas as pd
from pymongo import MongoClient
import pymongo
from backend.DB import eu
from backend.DB import db

########################################################################################################################
countries = ['NO', 'HR', 'HU', 'CH', 'CZ', 'RO', 'LV', 'GR', 'UK', 'SI', 'LT',
             'ES', 'FR', 'IE', 'SE', 'NL', 'PT', 'PL', 'DK', 'MK', 'DE', 'IT',
             'BG', 'CY', 'AT', 'LU', 'BE', 'FI', 'EE', 'SK', 'MT', 'LI', 'IS']


def ex0_cpv_example(bot_year=2008, top_year=2020):
    """
    Returns all contracts in given year 'YEAR' range and cap to 100000000 the 'VALUE_EURO'

    Expected Output (list of documents):
    [{'result': count_value(int)}]
    """

    def year_filter(bot_year, top_year):
        filter_ = {
            '$match': {
                '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                'VALUE_EURO': {'$lt': 100000000}
            }}

        return filter_

    count = {
        '$count': 'result'
    }

    pipeline = [year_filter(bot_year, top_year), count]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents

def year_country_filter(bot_year, top_year,country_list):
    filter_ = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}},{'ISO_COUNTRY_CODE': {'$in': country_list}}]
        }}
    return filter_
#in order to avoid repeating the logic to correct the CPV codes to all of the following queries, we do it in the function below:
def correct_CPV_codes():
    eu.update_many(
        {'CPV': {'$exists': True}},
        [
            {"$set": {"CPV": {'$toString': "$CPV"}}}
        ]
    )
    eu.update_many(
        {"CPV": {"$exists": True},
         "$expr": {"$lt": [{"$strLenCP": "$CPV"}, 8]}},
        [
            {'$set': {'CPV': {'$concat': ["0", "$CPV"]}}}
        ]
    )

#this was commented as it only needs to be run once to update the db
#correct_CPV_codes()
#check if it worked
# list(eu.find({
#     "CPV": { "$exists": True },
#     "$expr": { "$lt": [ { "$strLenCP": "$CPV" }, 8 ] }},{'CPV':1}
# ).limit(5))

def ex1_cpv_box(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns five metrics, described below
    Result filterable by floor year, roof year and country_list

    Expected Output:
    (avg_cpv_euro_avg, avg_cpv_count, avg_cpv_offer_avg, avg_cpv_euro_avg_y_eu, avg_cpv_euro_avg_n_eu)

    Where:
    avg_cpv_euro_avg = average value of each CPV's division contracts average 'VALUE_EURO', (int)
    avg_cpv_count = average value of each CPV's division contract count, (int)
    avg_cpv_offer_avg = average value of each CPV's division contracts average NUMBER_OFFERS', (int)
    avg_cpv_euro_avg_y_eu = average value of each CPV's division contracts average VALUE_EURO' with 'B_EU_FUNDS', (int)
    avg_cpv_euro_avg_n_eu = average value of each CPV's division contracts average 'VALUE_EURO' with out 'B_EU_FUNDS' (int)
    """

    average_value_cpv = {'$group': {
                '_id': {
                    'CPV': {'$substr': ['$CPV', 0, 2]}
                },
                'avg_val_CPV': {'$avg':'$VALUE_EURO'}
                    }
            }

    count_cpv = {'$group': {
                '_id': {
                    'CPV': {'$substr': ['$CPV', 0, 2]}
                },
                'count_CPV': {'$sum': 1}
                    }
            }

    average_offer_cpv = {'$group': {
                '_id': {
                    'CPV': {'$substr': ['$CPV', 0, 2]}
                },
                'avg_offer_CPV': {'$avg':'$NUMBER_OFFERS'}
                    }
            }

    eu_filter ={'$match': {"B_EU_FUNDS": {"$eq": "Y"}}}
    average_value_eu_cpv = {'$group': {
        '_id': {
            'CPV': {'$substr': ['$CPV', 0, 2]}
        },
        'avg_val_eu_CPV': {'$avg': '$VALUE_EURO'}
    }
    }
    noeu_filter ={'$match': {"B_EU_FUNDS": {"$eq": "N"}}}
    average_value_noeu_cpv = {'$group': {
        '_id': {
            'CPV': {'$substr': ['$CPV', 0, 2]}
        },
        'avg_val_noeu_CPV': {'$avg': '$VALUE_EURO'}
    }
    }

    def average_average(to_average):
        avg_avg_q = {'$group': {
                    '_id':None,
                    'avg_avg': {'$avg': to_average}}}
        return avg_avg_q

    pipeline_val_avg = [year_country_filter(bot_year, top_year, country_list), average_value_cpv, average_average('$avg_val_CPV')]
    pipeline_count = [year_country_filter(bot_year, top_year, country_list), count_cpv, average_average('$count_CPV')]
    pipeline_offer_avg = [year_country_filter(bot_year, top_year, country_list), average_offer_cpv, average_average('$avg_offer_CPV')]
    pipeline_val_eu_avg = [year_country_filter(bot_year, top_year, country_list), eu_filter, average_value_eu_cpv, average_average('$avg_val_eu_CPV')]
    pipeline_val_noeu_avg = [year_country_filter(bot_year, top_year, country_list),noeu_filter, average_value_noeu_cpv, average_average('$avg_val_noeu_CPV')]


    avg_cpv_euro_avg = int(list(eu.aggregate(pipeline_val_avg))[0]['avg_avg'])
    avg_cpv_count = int(list(eu.aggregate(pipeline_count))[0]['avg_avg'])
    avg_cpv_offer_avg = int(list(eu.aggregate(pipeline_offer_avg))[0]['avg_avg'])
    avg_cpv_euro_avg_y_eu = int(list(eu.aggregate(pipeline_val_eu_avg))[0]['avg_avg'])
    avg_cpv_euro_avg_n_eu = int(list(eu.aggregate(pipeline_val_noeu_avg))[0]['avg_avg'])

    return avg_cpv_euro_avg, avg_cpv_count, avg_cpv_offer_avg, avg_cpv_euro_avg_y_eu, avg_cpv_euro_avg_n_eu


def ex2_cpv_treemap(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the count of contracts for each CPV Division
    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{cpv: value_1, count: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = contract count of each CPV Division, (int)
    """

    count_cpv = {'$group': {
        '_id': {
            'cpv': {'$substr': ['$CPV', 0, 2]},
        },
        'count_contracts': {'$sum': 1}
    }
    }

    join_cpv_description ={'$lookup': {
        'from': 'cpv',
        'localField': '_id.cpv',
        'foreignField': 'cpv_division',
        'as': 'CPV_col'
    }}
    cpv_projection = {
        '$project': {
            '_id': False,
            'CPV_col': {'$arrayElemAt': ['$CPV_col', 0]},
            'count': '$count_contracts'
        }
    }

    cpv_desc_proj = {
        '$project': {
            '_id': False,
            'cpv': '$CPV_col.cpv_division_description',
            'count': '$count'
        }
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), count_cpv, join_cpv_description, cpv_projection, cpv_desc_proj]

    list_documents = list(eu.aggregate(pipeline))
    return list_documents


def ex3_cpv_bar_1(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'VALUE_EURO' return the highest 5 cpvs
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each CPV Division, (float)
    """
    count_cpv = {'$group': {
        '_id': {
            'cpv': {'$substr': ['$CPV', 0, 2]},
        },
        'average_val': {'$avg': '$VALUE_EURO'}
    }
    }

    join_cpv_description = {'$lookup': {
        'from': 'cpv',
        'localField': '_id.cpv',
        'foreignField': 'cpv_division',
        'as': 'CPV_col'
    }}
    cpv_projection = {
        '$project': {
            '_id': False,
            'CPV_col': {'$arrayElemAt': ['$CPV_col', 0]},
            'average': '$average_val'
        }
    }

    cpv_desc_proj = {
        '$project': {
            '_id': False,
            'cpv': '$CPV_col.cpv_division_description',
            'avg': '$average'
        }
    }
    cpv_sort = {
        '$sort': {
            'avg': -1
        }
    }
    cpv_limit = {
        '$limit': 5
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), count_cpv, join_cpv_description, cpv_projection, cpv_desc_proj, cpv_sort, cpv_limit]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex4_cpv_bar_2(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'VALUE_EURO' return the lowest 5 cpvs
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each CPV Division, (float)
    """

    count_cpv = {'$group': {
        '_id': {
            'cpv': {'$substr': ['$CPV', 0, 2]},
        },
        'average_val': {'$avg': '$VALUE_EURO'}
    }
    }

    join_cpv_description = {'$lookup': {
        'from': 'cpv',
        'localField': '_id.cpv',
        'foreignField': 'cpv_division',
        'as': 'CPV_col'
    }}
    cpv_projection = {
        '$project': {
            '_id': False,
            'CPV_col': {'$arrayElemAt': ['$CPV_col', 0]},
            'average': '$average_val'
        }
    }

    cpv_desc_proj = {
        '$project': {
            '_id': False,
            'cpv': '$CPV_col.cpv_division_description',
            'avg': '$average'
        }
    }
    cpv_sort = {
        '$sort': {
            'avg': 1
        }
    }
    cpv_limit = {
        '$limit': 5
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), count_cpv, join_cpv_description, cpv_projection,
                cpv_desc_proj, cpv_sort, cpv_limit]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex5_cpv_bar_3(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'VALUE_EURO' return the highest 5 cpvs for contracts which recieved european funds ('B_EU_FUNDS') 
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each CPV Division, (float)
    """
    eu_filter = {'$match': {"B_EU_FUNDS": {"$eq": "Y"}}}
    count_cpv = {'$group': {
        '_id': {
            'cpv': {'$substr': ['$CPV', 0, 2]},
        },
        'average_val': {'$avg': '$VALUE_EURO'}
    }
    }
    join_cpv_description = {'$lookup': {
        'from': 'cpv',
        'localField': '_id.cpv',
        'foreignField': 'cpv_division',
        'as': 'CPV_col'
    }}
    cpv_projection = {
        '$project': {
            '_id': False,
            'CPV_col': {'$arrayElemAt': ['$CPV_col', 0]},
            'average': '$average_val'
        }
    }

    cpv_desc_proj = {
        '$project': {
            '_id': False,
            'cpv': '$CPV_col.cpv_division_description',
            'avg': '$average'
        }
    }
    cpv_sort = {
        '$sort': {
            'avg': -1
        }
    }
    cpv_limit = {
        '$limit': 5
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list),eu_filter, count_cpv, join_cpv_description, cpv_projection, cpv_desc_proj, cpv_sort, cpv_limit]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex6_cpv_bar_4(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'VALUE_EURO' return the highest 5 cpvs for contracts which did not recieve european funds ('B_EU_FUNDS') 
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each CPV Division, (float)
    """
    noeu_filter = {'$match': {"B_EU_FUNDS": {"$eq": "N"}}}
    count_cpv = {'$group': {
        '_id': {
            'cpv': {'$substr': ['$CPV', 0, 2]},
        },
        'average_val': {'$avg': '$VALUE_EURO'}
    }
    }
    join_cpv_description = {'$lookup': {
        'from': 'cpv',
        'localField': '_id.cpv',
        'foreignField': 'cpv_division',
        'as': 'CPV_col'
    }}
    cpv_projection = {
        '$project': {
            '_id': False,
            'CPV_col': {'$arrayElemAt': ['$CPV_col', 0]},
            'average': '$average_val'
        }
    }

    cpv_desc_proj = {
        '$project': {
            '_id': False,
            'cpv': '$CPV_col.cpv_division_description',
            'avg': '$average'
        }
    }
    cpv_sort = {
        '$sort': {
            'avg': -1
        }
    }
    cpv_limit = {
        '$limit': 5
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list),noeu_filter, count_cpv, join_cpv_description, cpv_projection, cpv_desc_proj, cpv_sort, cpv_limit]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex7_cpv_map(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the highest CPV Division on average 'VALUE_EURO' per country 'ISO_COUNTRY_CODE'

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{cpv: value_1, avg: value_2, country: value_3}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = highest CPV Division average 'VALUE_EURO' of country, (float)
    value_3 = country in ISO-A2 format (string) (located in iso_codes collection)
    """
    count_cpv_iso = {
        '$group': {
            '_id': {
                'ISO_COUNTRY': '$ISO_COUNTRY_CODE',
                'cpv': {'$substr': ['$CPV', 0, 2]}
            },
            'average_val': {'$avg': '$VALUE_EURO'}
        }
    }

    sort_avg = {
        '$sort': {
            'average_val': -1
        }
    }

    top_cpv = {
        '$group': {
            '_id': {'country': '$_id.ISO_COUNTRY'},
            'cpv': {'$first': '$_id.cpv'},
            'avg': {'$max': '$average_val'}
        }
    }

    join_cpv_description = {
        '$lookup': {
            'from': 'cpv',
            'localField': 'cpv',
            'foreignField': 'cpv_division',
            'as': 'CPV_col'
        }
    }

    join_iso_description = {
        '$lookup': {
            'from': 'iso_codes',
            'localField': '_id.country',
            'foreignField': 'alpha-2',
            'as': 'ISO_col'
        }
    }

    iso_cpv_projection = {
        '$project': {
            '_id': False,
            'ISO_col': {'$arrayElemAt': ['$ISO_col', 0]},
            'CPV_col': {'$arrayElemAt': ['$CPV_col', 0]},
            'average': '$avg'
        }
    }


    iso_cpv_desc_proj = {
        '$project': {
            '_id': False,
            'country': '$ISO_col.iso_3166-2',
            'cpv': '$CPV_col.cpv_division_description',
            'avg': '$average'
        }
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), count_cpv_iso, sort_avg, top_cpv,
                join_cpv_description, join_iso_description, iso_cpv_projection, iso_cpv_desc_proj]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex8_cpv_hist(bot_year=2008, top_year=2020, country_list=countries, cpv='50'):
    """
    Produce an histogram where each bucket has the contract counts of a particular cpv
     in a given range of values (bucket) according to 'VALUE_EURO'

     Choose 10 buckets of any partition
    Buckets Example:
     0 to 100000
     100000 to 200000
     200000 to 300000
     300000 to 400000
     400000 to 500000
     500000 to 600000
     600000 to 700000
     700000 to 800000
     800000 to 900000
     900000 to 1000000


    So given a CPV Division code (two digit string) return a list of documents where each document as the bucket _id,
    and respective bucket count.

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{bucket: value_1, count: value_2}, ....]

    Where:
    value_1 = lower limit of respective bucket (if bucket position 0 of example then bucket:0 )
    value_2 = contract count for thar particular bucket, (int)
    """

    filter_cpv = {
        '$match': {
            '$expr': {'$eq': [{'$substr': ['$CPV', 0, 2]}, cpv]}
        }
    }

    bucket_value = {
        '$bucket': {
            'groupBy': '$VALUE_EURO',
            'boundaries': [0, 100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000],
            'default': '-',  #default - ou 0?
            'output': {
                'count': {'$sum': 1}
            }
        }
    }

    project_bucket = {
        '$project': {
            '_id': False,
            'bucket': '$_id',
            'count': '$count'
        }
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), filter_cpv, bucket_value, project_bucket]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents

def ex9_cpv_bar_diff(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average time and value difference for each CPV, return the highest 5 cpvs

    time difference = 'DT-DISPATCH' - 'DT-AWARD'
    value difference = 'AWARD_VALUE_EURO' - 'VALUE_EURO'

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{cpv: value_1, time_difference: value_2, value_difference: value_3}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'DT-DISPACH' - 'DT-AWARD', (float)
    value_3 = average 'EURO_AWARD' - 'VALUE_EURO' (float)
    """
    dates_to_string = {
        '$project': {
            'cpv': {'$substr': ['$CPV', 0, 2]},
            'DT_DISPATCH': {'$dateFromString': {'dateString': '$DT_DISPATCH'}},
            'DT_AWARD': {'$dateFromString': {'dateString': '$DT_AWARD'}},
            'AWARD_VALUE_EURO': '$AWARD_VALUE_EURO',
            'VALUE_EURO': '$VALUE_EURO'
        }
    }

    projection = {
        '$project': {
            '_id': 0,
            'cpv': 1,
            'time_diff': {'$subtract': ["$DT_DISPATCH","$DT_AWARD"]},
            'value_diff': {'$subtract': ["$AWARD_VALUE_EURO", "$VALUE_EURO"]}
        }
    }

    cpv_avg = {
        '$group': {
            '_id': '$cpv',
            'time_diff_avg': {'$avg': '$time_diff'},
            'value_diff_avg': {'$avg': '$value_diff'}
        }
    }

    join_cpv_description = {
        '$lookup': {
            'from': 'cpv',
            'localField': '_id',
            'foreignField': 'cpv_division',
            'as': 'CPV_col'
        }
    }

    cpv_projection = {
        '$project': {
            '_id': False,
            'CPV_col': {'$arrayElemAt': ['$CPV_col', 0]},
            'time_difference':'$time_diff_avg',
            'value_difference':'$value_diff_avg'
        }
    }

    cpv_desc_proj = {
        '$project': {
            '_id': False,
            'cpv': '$CPV_col.cpv_division_description',
            'time_difference': '$time_difference',
            'value_difference': '$value_difference'
        }
    }

    sort = {
        '$sort': {
            'time_difference': -1,
            'value_difference': -1

        }
    }

    cpv_limit = {
        '$limit': 5
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), dates_to_string, projection, cpv_avg,
                join_cpv_description, cpv_projection, cpv_desc_proj, sort, cpv_limit]

    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents

def ex10_country_box(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want five numbers, described below
    Result filterable by floor year, roof year and country_list

    Expected Output:
    (avg_country_euro_avg, avg_country_count, avg_country_offer_avg, avg_country_euro_avg_y_eu, avg_country_euro_avg_n_eu)

    Where:
    avg_country_euro_avg = average value of each countries ('ISO_COUNTRY_CODE') contracts average 'VALUE_EURO', (int)
    avg_country_count = average value of each countries ('ISO_COUNTRY_CODE') contract count, (int)
    avg_country_offer_avg = average value of each countries ('ISO_COUNTRY_CODE') contracts average NUMBER_OFFERS', (int)
    avg_country_euro_avg_y_eu = average value of each countries ('ISO_COUNTRY_CODE') contracts average VALUE_EURO' with 'B_EU_FUNDS', (int)
    avg_country_euro_avg_n_eu = average value of each countries ('ISO_COUNTRY_CODE') contracts average 'VALUE_EURO' with out 'B_EU_FUNDS' (int)
    """
    val_not_null = {
        "$match": {
            "VALUE_EURO": {
                "$exists": True,
                "$gte": 0
            }
        }
    }
    average_value_country = {'$group': {
                '_id': {
                    'country': '$ISO_COUNTRY_CODE'
                },
                'avg_val_country': {'$avg':'$VALUE_EURO'}
                    }
            }

    count_country = {'$group': {
                '_id': {
                    'country': '$ISO_COUNTRY_CODE'
                },
                'count_country': {'$sum': 1}
                    }
            }

    average_offer_country = {'$group': {
                '_id': {
                    'country': '$ISO_COUNTRY_CODE'
                },
                'avg_offer_country': {'$avg':'$NUMBER_OFFERS'}
                    }
            }

    eu_filter = {'$match': {"B_EU_FUNDS": {"$eq": "Y"}}}

    average_value_eu_country = {'$group': {
        '_id': {
            'country': '$ISO_COUNTRY_CODE'
        },
        'avg_val_eu_country': {'$avg': '$VALUE_EURO'}
    }
    }

    noeu_filter = {'$match': {"B_EU_FUNDS": {"$eq": "N"}}}

    average_value_noeu_country = {'$group': {
        '_id': {
            'country': '$ISO_COUNTRY_CODE'
        },
        'avg_val_noeu_country': {'$avg': '$VALUE_EURO'}
    }
    }

    def average_average(to_average):
        avg_avg_q = {'$group': {
                    '_id':None,
                    'avg_avg': {'$avg': to_average}}}
        return avg_avg_q

    pipeline_val_avg = [year_country_filter(bot_year, top_year, country_list),val_not_null, average_value_country, average_average('$avg_val_country')]
    pipeline_count = [year_country_filter(bot_year, top_year, country_list),val_not_null, count_country, average_average('$count_country')]
    pipeline_offer_avg = [year_country_filter(bot_year, top_year, country_list),val_not_null, average_offer_country, average_average('$avg_offer_country')]
    pipeline_val_eu_avg = [year_country_filter(bot_year, top_year, country_list),val_not_null, eu_filter, average_value_eu_country, average_average('$avg_val_eu_country')]
    pipeline_val_noeu_avg = [year_country_filter(bot_year, top_year, country_list),val_not_null,noeu_filter, average_value_noeu_country, average_average('$avg_val_noeu_country')]


    avg_country_euro_avg = int(list(eu.aggregate(pipeline_val_avg))[0]['avg_avg'])
    avg_country_count = int(list(eu.aggregate(pipeline_count))[0]['avg_avg'])
    avg_country_offer_avg = int(list(eu.aggregate(pipeline_offer_avg))[0]['avg_avg'])
    avg_country_euro_avg_y_eu = int(list(eu.aggregate(pipeline_val_eu_avg))[0]['avg_avg'])
    avg_country_euro_avg_n_eu = int(list(eu.aggregate(pipeline_val_noeu_avg))[0]['avg_avg'])

    return avg_country_euro_avg, avg_country_count, avg_country_offer_avg, avg_country_euro_avg_y_eu, avg_country_euro_avg_n_eu



def ex11_country_treemap(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the count of contracts per country ('ISO_COUNTRY_CODE')
    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{country: value_1, count: value_2}, ....]

    Where:
    value_1 = Country ('ISO_COUNTRY_CODE') name, (string) (located in iso_codes collection')
    value_2 = contract count of each country, (int)
    """

    count_country = {'$group': {'_id': {'country': '$ISO_COUNTRY_CODE'},
                    'count_contracts': {'$sum': 1}
                    }
                     }

    join_country_name ={'$lookup': {
        'from': 'iso_codes',
        'localField': '_id.country',
        'foreignField': 'alpha-2',
        'as': 'name_col'
    }}

    country_projection = {
        '$project': {
            '_id': False,
            'name_col': {'$arrayElemAt': ['$name_col', 0]},
            'count': '$count_contracts'
        }
    }

    country_name_proj = {
        '$project': {
            '_id': False,
            'cpv': '$name_col.name',
            'avg': '$average'
        }
    }


    pipeline = [year_country_filter(bot_year, top_year, country_list), count_country, join_country_name, country_projection, country_name_proj]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex12_country_bar_1(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average 'VALUE_EURO' for each country, return the highest 5 countries

    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{country: value_1, avg: value_2}, ....]

    Where:
    value_1 = Country ('ISO_COUNTRY_CODE') name, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each country ('ISO_COUNTRY_CODE') name, (float)
    """

    average_country = {'$group': {'_id': {'country': '$ISO_COUNTRY_CODE'},
                                'average_val': {'$avg': '$VALUE_EURO'}
                                }
                     }

    join_country_name = {'$lookup': {
        'from': 'iso_codes',
        'localField': '_id.country',
        'foreignField': 'alpha-2',
        'as': 'name_col'
    }}

    country_projection = {
        '$project': {
            '_id': False,
            'name_col': {'$arrayElemAt': ['$name_col', 0]},
            'average': '$average_val'
        }
    }

    country_name_proj = {
        '$project': {
            '_id': False,
            'country': '$name_col.name',
            'avg': '$average'
        }
    }
    country_sort = {
        '$sort': {
            'avg': -1
        }
    }
    country_limit = {
        '$limit': 5
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), average_country, join_country_name,country_projection, country_name_proj, country_sort, country_limit]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex13_country_bar_2(bot_year=2008, top_year=2020, country_list=countries):
    """
    Group by country and get the average 'VALUE_EURO' for each group, return the lowest, average wise, 5 documents

    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{country: value_1, avg: value_2}, ....]

    Where:
    value_1 = Country ('ISO_COUNTRY_CODE') name, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'VALUE_EURO' of each country ('ISO_COUNTRY_CODE') name, (float)
    """

    average_country = {'$group': {'_id': {'country': '$ISO_COUNTRY_CODE'},
                                  'average_val': {'$avg': '$VALUE_EURO'}
                                  }
                       }

    join_country_name = {'$lookup': {
        'from': 'iso_codes',
        'localField': '_id.country',
        'foreignField': 'alpha-2',
        'as': 'name_col'
    }}

    country_projection = {
        '$project': {
            '_id': False,
            'name_col': {'$arrayElemAt': ['$name_col', 0]},
            'average': '$average_val'
        }
    }

    country_name_proj = {
        '$project': {
            '_id': False,
            'country': '$name_col.name',
            'avg': '$average'
        }
    }
    country_sort = {
        '$sort': {
            'avg': 1
        }
    }
    country_limit = {
        '$limit': 5
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), average_country, join_country_name,
                country_projection, country_name_proj, country_sort, country_limit]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex14_country_map(bot_year=2008, top_year=2020, country_list=countries):
    """
    For each country get the sum of the respective contracts 'VALUE_EURO' with 'B_EU_FUNDS'

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{sum: value_1, country: value_2}, ....]

    Where:
    value_1 = sum 'VALUE_EURO' of country ('ISO_COUNTRY_CODE') name, (float)
    value_2 = country in ISO-A2 format (string) (located in iso_codes collection)
    """
    eu_filter = {'$match': {"B_EU_FUNDS": {"$eq": "Y"}}}

    sum_country = {'$group': {'_id': {'country': '$ISO_COUNTRY_CODE'},
                                'sum_val': {'$sum': '$VALUE_EURO'}
                                }
                   }

    join_iso_codes = {'$lookup': {
        'from': 'iso_codes',
        'localField': '_id.country',
        'foreignField': 'alpha-2',
        'as': 'iso'
    }
    }

    iso_projection = {
        '$project': {
            '_id': 0,
            'sum': '$sum_val',
            'iso': {'$arrayElemAt': ['$iso', 0]}
        }
    }

    country_proj = {
        '$project': {
            '_id': 0,
            'sum': '$sum',
            'country': "$iso.alpha-2"
        }
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), eu_filter, sum_country,join_iso_codes,iso_projection, country_proj]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex15_business_box(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want five numbers, described below

    Result filterable by floor year, roof year and country_list

    Expected Output:
    (avg_business_euro_avg, avg_business_count, avg_business_offer_avg, avg_business_euro_avg_y_eu, avg_business_euro_avg_n_eu)

    Where:
    avg_business_euro_avg = average value of each company ('CAE_NAME')  contracts average 'VALUE_EURO', (int)
    avg_business_count = average value of each company ('CAE_NAME') contract count, (int)
    avg_business_offer_avg = average value of each company ('CAE_NAME') contracts average NUMBER_OFFERS', (int)
    avg_business_euro_avg_y_eu = average value of each company ('CAE_NAME') contracts average VALUE_EURO' with 'B_EU_FUNDS', (int)
    avg_business_euro_avg_n_eu = average value of each company ('CAE_NAME') contracts average 'VALUE_EURO' with out 'B_EU_FUNDS' (int)
    """
    average_value_bus = {'$group': {
                '_id': {
                    'bus': '$CAE_NAME'
                },
                'avg_val_bus': {'$avg':'$VALUE_EURO'}
                    }
            }

    count_bus = {'$group': {
                '_id': {
                    'bus': '$CAE_NAME'
                },
                'count_bus': {'$sum': 1}
                    }
            }

    average_offer_bus = {'$group': {
                '_id': {
                    'bus': '$CAE_NAME'
                },
                'avg_offer_bus': {'$avg':'$NUMBER_OFFERS'}
                    }
            }

    eu_filter = {'$match': {"B_EU_FUNDS": {"$eq": "Y"}}}

    average_value_eu_bus = {'$group': {
        '_id': {
            'bus': '$CAE_NAME'
        },
        'avg_val_eu_bus': {'$avg': '$VALUE_EURO'}
    }
    }

    noeu_filter = {'$match': {"B_EU_FUNDS": {"$eq": "N"}}}

    average_value_noeu_bus = {'$group': {
        '_id': {
            'bus': '$CAE_NAME'
        },
        'avg_val_noeu_bus': {'$avg': '$VALUE_EURO'}
    }
    }

    def average_average(to_average):
        avg_avg_q = {'$group': {
                    '_id':None,
                    'avg_avg': {'$avg': to_average}}}
        return avg_avg_q

    pipeline_val_avg = [year_country_filter(bot_year, top_year, country_list), average_value_bus, average_average('$avg_val_bus')]
    pipeline_count = [year_country_filter(bot_year, top_year, country_list), count_bus, average_average('$count_bus')]
    pipeline_offer_avg = [year_country_filter(bot_year, top_year, country_list), average_offer_bus, average_average('$avg_offer_bus')]
    pipeline_val_eu_avg = [year_country_filter(bot_year, top_year, country_list), eu_filter, average_value_eu_bus, average_average('$avg_val_eu_bus')]
    pipeline_val_noeu_avg = [year_country_filter(bot_year, top_year, country_list),noeu_filter, average_value_noeu_bus, average_average('$avg_val_noeu_bus')]


    avg_business_euro_avg = int(list(eu.aggregate(pipeline_val_avg))[0]['avg_avg'])
    avg_business_count = int(list(eu.aggregate(pipeline_count))[0]['avg_avg'])
    avg_business_offer_avg = int(list(eu.aggregate(pipeline_offer_avg))[0]['avg_avg'])
    avg_business_euro_avg_y_eu = int(list(eu.aggregate(pipeline_val_eu_avg))[0]['avg_avg'])
    avg_business_euro_avg_n_eu = int(list(eu.aggregate(pipeline_val_noeu_avg))[0]['avg_avg'])

    return avg_business_euro_avg, avg_business_count, avg_business_offer_avg, avg_business_euro_avg_y_eu, avg_business_euro_avg_n_eu


def ex16_business_bar_1(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average 'VALUE_EURO' for company ('CAE_NAME') return the highest 5 companies
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{company: value_1, avg: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME') name, (string)
    value_2 = average 'VALUE_EURO' of each company ('CAE_NAME'), (float)
    """

    average_bus = {'$group': {'_id': {'bus': '$CAE_NAME'},
                              'average': {'$avg': '$VALUE_EURO'}
                              }
                   }
    bus_name_proj = {
        '$project': {
            '_id': False,
            'company': '$_id.bus',
            'avg': '$average'
        }
    }
    bus_sort = {
        '$sort': {
            'avg': -1
        }
    }
    bus_limit = {
        '$limit': 5
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), average_bus, bus_name_proj, bus_sort, bus_limit]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex17_business_bar_2(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average 'VALUE_EURO' for company ('CAE_NAME') return the lowest 5 companies


    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{company: value_1, avg: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME') name, (string)
    value_2 = average 'VALUE_EURO' of each company ('CAE_NAME'), (float)
    """
    val_not_null ={
        "$match": {
            "VALUE_EURO": {
                "$exists": True,
                 "$gte": 0
            }
        }
    }
    average_bus = {'$group': {'_id': {'bus': '$CAE_NAME'},
                              'average': {'$avg': '$VALUE_EURO'}
                              }
                   }
    bus_name_proj = {
        '$project': {
            '_id': False,
            'company': '$_id.bus',
            'avg': '$average'
        }
    }
    bus_sort = {
        '$sort': {
            'avg': 1
        }
    }
    bus_limit = {
        '$limit': 5
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), val_not_null, average_bus, bus_name_proj, bus_sort, bus_limit]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex18_business_treemap(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want the count of contracts for each company 'CAE_NAME', for the highest 15
    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{company: value_1, count: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME'), (string)
    value_2 = contract count of each company ('CAE_NAME'), (int)
    """

    count_bus = {'$group': {
                '_id': {
                    'bus': '$CAE_NAME'
                },
                'count_bus': {'$sum': 1}
                    }
            }
    bus_name_proj = {
        '$project': {
            '_id': False,
            'company': '$_id.bus',
            'count': {'$toInt':'$count_bus'}
        }
    }
    bus_sort = {
        '$sort': {
            'count': -1
        }
    }
    bus_limit = {
        '$limit': 15
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), count_bus,bus_name_proj, bus_sort, bus_limit]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents



def ex19_business_map(bot_year=2008, top_year=2020, country_list=countries):
    """
    For each country get the highest company ('CAE_NAME') in terms of 'VALUE_EURO' sum contract spending

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{company: value_1, sum: value_2, country: value_3, address: value_4}, ....]

    Where:
    value_1 = 'top' company of that particular country ('CAE_NAME'), (string)
    value_2 = sum 'VALUE_EURO' of country and company ('CAE_NAME'), (float)
    value_3 = country in ISO-A2 format (string) (located in iso_codes collection)
    value_4 = company ('CAE_NAME') address, single string merging 'CAE_ADDRESS' and 'CAE_TOWN' separated by ' ' (space)
    """

    sum_value = {
        '$group': {
            '_id': {'country': '$ISO_COUNTRY_CODE', 'company': '$CAE_NAME'},
            'sum': {'$sum': '$VALUE_EURO'},
            'address': {'$first': {'$concat': [{"$toString": "$CAE_ADDRESS"}, " ", {"$toString": "$CAE_TOWN"}]}}
        }}

    sort_sum = {'$sort': {'sum': -1}}

    top_company = {'$group': {
        '_id': {'country': '$_id.country'},
        'sum': {'$max': '$sum'},
        'company': {'$first': '$_id.company'},
        'address': {'$first': '$address'}
    }}

    join_iso_codes = {'$lookup': {
        'from': 'iso_codes',
        'localField': '_id.country',
        'foreignField': 'alpha-2',
        'as': 'iso'
    }
    }

    iso_projection = {
        '$project': {
            '_id': 0,
            'company': "$company",
            'sum': '$sum',
            'iso': {'$arrayElemAt': ['$iso', 0]},
            'address': '$address'
        }
    }

    iso_2 = {
        '$project': {
            '_id': 0,
            'company': "$company",
            'sum': '$sum',
            'country': "$iso.alpha-2",
            'address': '$address'
        }
    }

    pipeline = [year_country_filter(bot_year, top_year, country_list), sum_value, sort_sum, top_company, join_iso_codes,
                iso_projection, iso_2]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents



def ex20_business_connection(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want the top 5 most frequent co-occurring companies ('CAE_NAME' and 'WIN_NAME')

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{companies: value_1, count: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME') string merged with company ('WIN_NAME') seperated by the string ' with ', (string)
    value_2 = co-occurring number of contracts (int)
    """
    filter_out_none = {
        "$match": {
            "CAE_NAME": {
                "$exists": True,
                "$ne": None
            },
            "WIN_NAME": {
                "$exists": True,
                "$ne": None
            }
        }
    }

    count_occ = {'$group': {
        '_id': {'company': '$CAE_NAME', 'company_win': '$WIN_NAME'},
        'count_1': {'$sum': 1}
        }
    }

    merge_company = {
        '$project': {
            '_id':0,
            'companies': {'$concat': [{"$toString": "$_id.company"}, " with ", {"$toString": "$_id.company_win"}]},
            'count': '$count_1'
        }
    }

    sort_count = {
        '$sort': {'count': -1}
    }
    company_limit = {'$limit': 5}

    pipeline = [year_country_filter(bot_year, top_year, country_list), filter_out_none,count_occ,merge_company,sort_count, company_limit]

    list_documents = list(eu.aggregate(pipeline, allowDiskUse=True))

    return list_documents

ex20_business_connection()


def insert_operation(document):
    '''
        Insert operation.

        In case pre computed tables were generated for the queries they should be recomputed with the new data.
    '''

    inserted_ids = eu.insert_many(document).inserted_ids

    return inserted_ids


query_list = [
    ex1_cpv_box, ex2_cpv_treemap, ex3_cpv_bar_1, ex4_cpv_bar_2,
    ex5_cpv_bar_3, ex6_cpv_bar_4, ex7_cpv_map, ex8_cpv_hist ,ex9_cpv_bar_diff,
    ex10_country_box, ex11_country_treemap, ex12_country_bar_1,
    ex13_country_bar_2, ex14_country_map, ex15_business_box,
    ex16_business_bar_1, ex17_business_bar_2, ex18_business_treemap,
    ex19_business_map, ex20_business_connection
]
