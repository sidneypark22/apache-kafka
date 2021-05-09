import requests
import pymetservices
import pymetservices as pm
import json
import datetime as dt

help(pm)

def searchDailyWeatherByCity(cityToSearch):
    dailyList = []
    daily = pm.getDaily(city=cityToSearch)
    counter_1 = 0
    while counter_1 < len(daily):
        counter_2 = 0
        while counter_2 < len(daily[counter_1]['data']):
            daily[counter_1]['data'][counter_2]['location'] = daily[counter_1]['location']
            counter_2 += 1
        for data in daily[counter_1]['data']:
            dailyList.append(data)
        counter_1 += 1
    return dailyList


def searchDailyWeather(**context):
    cityToSearch = ['auckland', 'wellington', 'hamilton', 'napier', 'christchurch', 'invercargill', 'queenstown']
    outputList = []
    for city in cityToSearch:
        output = searchDailyWeatherByCity(city)
        for row in output:
            outputList.append(row)
    #task_instance = 
    context['task_instance'].xcom_push(key='weather_data', value=outputList)
    #return outputList
    