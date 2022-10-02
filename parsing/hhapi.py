import pandas as pd
import numpy as np
import time
import requests

VACANCIES_PER_PAGE = 100

API_AREAS_REQUEST = 'https://api.hh.ru/areas'
API_VACANCIES_REQUEST = 'https://api.hh.ru/vacancies'

CURRANCY_USD = "USD"
CURRANCY_EUR = "EUR"

EXCHANGE_RATE_USD = 60
EXCHANGE_RATE_EUR = 70

TAX_RATE = 0.13

COUNTRY_RUSSIA = 'Россия'

COMMENT_REGION_PROCESSING_START = 'Обрабатываю регионы:'
COMMENT_REGION_PROCESSING_FINISH = 'Готово! Найдено вакансий: '
COMMENT_KEYWORD_PROCESSING_START = 'Ищу вакансии по ключевому слову '

def get_russian_regions():
    ''' Возвращает DataFrame регионов РФ'''
    regions = requests.get(API_AREAS_REQUEST)
    
    region_frame = pd.DataFrame(regions.json())
    
    russia_index = region_frame[region_frame['name'] == COUNTRY_RUSSIA].index.tolist()[0]
    russia_regions_frame = pd.DataFrame(region_frame['areas'][russia_index])
    
    return russia_regions_frame

# Возвращает количество вакансий в регионе region, 
# найденных по ключевому слову keyword,
def get_region_vacancies_count(keyword, region):
    result = requests.get(
        API_VACANCIES_REQUEST, 
        params = {
            'text': keyword, 
            'per_page' : 1, 
            'search_field' : 'description', 
            'area': region
        }
    )
    
    if(result.status_code == 200):
        return int(result.json()['found'])

    return 0    

### =================== Функции выгрузки вакансий, если их меньше 2000 ===================

# Возвращает список вакансий в регионе region, 
# найденных по ключевому слову keyword
def get_region_vacancies(keyword, region, page):

    response = requests.get(
        API_VACANCIES_REQUEST,
        params = {
            'text': keyword, 
            'area': region, 
            'per_page' : VACANCIES_PER_PAGE, 
            'search_field' : 'description', 
            'page': page
            }
    )
    if(response.status_code == 200):
        found_vacancies = response.json()['items']

        return found_vacancies
    
    return None

# Возвращает DataFrame вакансий, найденных по ключевым словам из списка keyword_list
def get_vacancies_data_frame(keyword_list, region = None):
    vacancies_list = []
    
    russia_regions_frame = get_russian_regions()

    if (region is not None):
        russia_regions_list = [region]
    else: 
        russia_regions_list = [x for x in russia_regions_frame['id']]

    for keyword in [keyword_list]:
        
        print(COMMENT_KEYWORD_PROCESSING_START + keyword)
        print(COMMENT_REGION_PROCESSING_START)
            
        for region in russia_regions_list:
            found = get_region_vacancies_count(keyword, region)
            print(found)
            region_name = russia_regions_frame.loc[russia_regions_frame['id'] == region]['name'].values[0];
            print(region_name)
            for page in range((found // VACANCIES_PER_PAGE) + 1):
                found_vacancies = get_region_vacancies(keyword, region,page)
                time.sleep(1)
                found_vacancies = enrich_vacancies_list(found_vacancies, region_name)

                if (found_vacancies is not None):
                    vacancies_list = vacancies_list + found_vacancies

    print(COMMENT_REGION_PROCESSING_FINISH)
    print(len(vacancies_list))
    
    full_vacancies_frame = pd.DataFrame(vacancies_list)
    
    return full_vacancies_frame

# Расширенная выгрузка вакансий

# Возвращает вакансию в виде словаря по ее id
def get_vacancy(id):
    response = requests.get(API_VACANCIES_REQUEST + "/" + str(id))
    
    if(response.status_code == 200):
        return response.json()
    
    return None

# Обогащает вакансии из vacancies данными из детальной информации по каждой 
# вакансии, а также названием региона region_name
def enrich_vacancies_list(vacancies, region_name):
    num = 0
    for vacancy in vacancies:
      try:
        if num < 10:
          num += 1
        else:
          print('*', end='')
          num = 0
        details = get_vacancy(vacancy['id'])
        vacancy['description'] = details['description']
        vacancy['experience'] = details['experience']
        vacancy['key_skills'] = details['key_skills']
        vacancy['specializations'] = details['specializations']
      
        vacancy['region'] = region_name
      except:
        print('exeption: ', vacancy)
        
    print('')
    return vacancies
    
# Для того, чтобы выгрузить больше двух тысяч вакансий в регионе, детализируем запрос:

# Соберем в список интервалы дат, из которых будем строить выгрузки. 
# Чем ближе к сегодняшнему дню, тем короче интервал.
date_from_to = [('2022-10-01', '2022-10-02'),
                ('2022-09-29', '2022-09-30'),
                ('2022-09-27', '2022-09-28'),
                ('2022-09-25', '2022-09-26'),
                ('2022-09-22', '2022-09-24'),
                ('2022-09-20', '2022-09-21'),
                ('2022-09-17', '2022-09-19'),
                ('2022-09-09', '2022-09-16'),
                ('2022-08-25', '2022-09-08'),
                ('2022-07-25', '2022-08-24')]
                
# Возвращает количество вакансий в регионе region, во временном интервале (date_from, date_to)
# найденных по ключевому слову keyword
def get_region_date_vacancies_count(keyword, region, date_from, date_to):
    result = requests.get(
        API_VACANCIES_REQUEST, 
        params = {
            'text': keyword, 
            'per_page' : 1, 
            'search_field' : 'description', 
            'area': region,
            'date_from': date_from,
            'date_to': date_to
        }
    )
            
    if(result.status_code == 200):
        return int(result.json()['found'])

    return 0    

# Добавляем параметры поиска: date_from и date_to
def get_region_vacancies(keyword, region, page, date_from, date_to):
    response = requests.get(
        API_VACANCIES_REQUEST,
        params = {
            'text': keyword, 
            'area': region, 
            'per_page' : VACANCIES_PER_PAGE, 
            'search_field' : 'description', 
            'page': page,
            # Добавляем новые параметры поиска
            'date_from': date_from,
            'date_to': date_to
            },
        headers = {'User-Agent': 'HH-User-Agent'}
    )
    
    if(response.status_code == 200):
        print(str(page) + ": get_region_vacancies is code 200 OK")
        found_vacancies = response.json()['items']
        return found_vacancies
    print(str(page) + response.url)
    print(response.status_code)    
    return None

#Функция получения дата-фрейма с вакансиями
def get_vacancies_data_frame(keyword_list, region = None):
    vacancies_list = []
    russia_regions_frame = get_russian_regions()
    if (region is not None):
        russia_regions_list = [region]
    else: 
        russia_regions_list = [x for x in russia_regions_frame['id']]

    for keyword in [keyword_list]:   
        print(COMMENT_KEYWORD_PROCESSING_START + keyword)
        print(COMMENT_REGION_PROCESSING_START)

        for region in russia_regions_list:
            region_name = russia_regions_frame.loc[russia_regions_frame['id'] == region]['name'].values[0];
            print(region_name)
            found = get_region_vacancies_count(keyword, region)
            # Если количество найденных вакансий больше 2000, выгружаем отдельно по каждым временным интервалам
            print('Всего вакансий по данному ключевому слову в регионе: '+ str(found))
            if found > 2000:
                for date_from, date_to in date_from_to:
                    found_date = get_region_date_vacancies_count(keyword, region, date_from, date_to)
                    print(date_from, date_to, 'total: ', found_date)
                    for page in range((found_date // VACANCIES_PER_PAGE) + 1):
                        found_vacancies = get_region_vacancies(keyword, region, page, date_from, date_to)
                        time.sleep(1)
                        found_vacancies = enrich_vacancies_list(found_vacancies, region_name)
                        if (found_vacancies is not None):
                            vacancies_list = vacancies_list + found_vacancies
            # Иначе грузим как обычно
            else:
                for page in range((found // VACANCIES_PER_PAGE) + 1):
                    found_vacancies = get_region_vacancies(keyword, region, page, date_from=None, date_to=None, experience=None, schedule=None)
                    time.sleep(1)
                    found_vacancies = enrich_vacancies_list(found_vacancies, region_name)

                    if (found_vacancies is not None):
                        vacancies_list = vacancies_list + found_vacancies

    print(COMMENT_REGION_PROCESSING_FINISH)
    print(len(vacancies_list))
    
    full_vacancies_frame = pd.DataFrame(vacancies_list)
    
    return full_vacancies_frame