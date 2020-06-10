import pandas as pd
from pymongo import MongoClient
from DB import eu
from DB import db

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
    

    pipeline = [year_country_filter(bot_year, top_year,country_list),]

    list_documents = []

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
    pipeline = []

    list_documents = []

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

    pipeline = []

    list_documents = []

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

    pipeline = []

    list_documents = []

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


    pipeline = []

    list_documents = []

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

    pipeline = []

    list_documents = []

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

    pipeline = []

    list_documents = []

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

    pipeline = []

    list_documents = []

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

    pipeline_val_avg = [year_country_filter(bot_year, top_year, country_list), average_value_country, average_average('$avg_val_country')]
    pipeline_count = [year_country_filter(bot_year, top_year, country_list), count_country, average_average('$count_country')]
    pipeline_offer_avg = [year_country_filter(bot_year, top_year, country_list), average_offer_country, average_average('$avg_offer_country')]
    pipeline_val_eu_avg = [year_country_filter(bot_year, top_year, country_list), eu_filter, average_value_eu_country, average_average('$avg_val_eu_country')]
    pipeline_val_noeu_avg = [year_country_filter(bot_year, top_year, country_list),noeu_filter, average_value_noeu_country, average_average('$avg_val_noeu_country')]


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

    pipeline = []

    list_documents = []

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
    query = {
        '$group': {
            '_id': {'country' : '$ISO_COUNTRY_CODE'}, # Ther group's id field is/are the keys that we want to group the data by
            'average_q12' : {'$avg' : '$VALUE_EURO'}            # A new key called 'count' that for each unique 'initial_letter' sums 1 to that particular value
        }
    }
    query_2 = {
        '$sort': {
            'average_q12': -1
        }
    }
    query_3 = {
        '$limit': 5
        }
    pipeline = [year_country_filter(bot_year, top_year,country_list),query,query_2,query_3]

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
    query = {
        '$group': {
            '_id': {'country' : '$ISO_COUNTRY_CODE'}, # Ther group's id field is/are the keys that we want to group the data by
            'average_q13' : {'$avg' : '$VALUE_EURO'}            # A new key called 'count' that for each unique 'initial_letter' sums 1 to that particular value
        }
    }
    query_2 = {
        '$sort': {
            'average_q13': 1
        }
    }
    query_3 = {
        '$limit': 5
        }
    pipeline = [year_country_filter(bot_year, top_year,country_list),query,query_2,query_3]

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

    query_2 = {
        '$group': {
            '_id': {'country' : '$ISO_COUNTRY_CODE'},
            """# Ther group's id field is/are the keys that we want to group the data by
            'sum_q14value' : {'$sum' : '$VALUE_EURO'},
            'sum_q14funds' : {'$sum' : '$B_EU_FUNDS'},
            """
            'final_sum' : {'$add':['$sum' : '$VALUE_EURO','$sum' : '$B_EU_FUNDS']}
            }
        }
    
    pipeline = [year_country_filter(bot_year, top_year,country_list),query_2]

    list_documents = [eu.aggregate(pipeline)]

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

    list_documents = ex3_cpv_bar_1(bot_year=2008, top_year=2020, country_list=countries)

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

    pipeline = []

    list_documents = []

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

    pipeline = []

    list_documents = []

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

    pipeline = []

    list_documents = []

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

    pipeline = []

    list_documents = []

    return list_documents

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
