# Импорт библиотек
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

# Список адресов для парсинга
url = ['https://obrazoval.ru/f/programmirovanie','https://obrazoval.ru/f/analitika']

# Количество страниц парсинга для каждого адреса
length = [18,10]

# Метки категорий курсов "Программирование" и "Аналитика"
label = ['prog','analit']

names = [] # Названия курсов
rates = [] # Оценки
prices = [] # Стоимость
durations = [] # Продолжительность
companies = [] # Название компании
labels = [] # Метки категорий

# Перебираем адреса
for i in range(2):
  print(url[i])
  # Перебираем страницы
  for page in range(1,length[i]):
    # Делаем запрос, выводим номер страницы и код ответа
    response = requests.get(url[i], params={'page': page})
    print(str(page) + ' : ' + str(response.status_code)) 

    # Находим нужные нам элементы с информацией о курсах
    soup = BeautifulSoup(response.text, 'lxml')
    courses = soup.find_all('div', class_='l-course b-bordered b-courses__course')

    # Перебираем курсы
    for course in courses:
        
        # Собираем названия курсов
        name = course.find('span', class_='ellipsis-3-lines')
        names.append(name.text)
        
        # Собираем оценки
        rate = course.find('div', class_='text-h4 q-mr-xs')
        rates.append(rate.text)
        
        # Собираем цены
        price = course.find('div', class_='b-title__custom l-course__price').find('span')
        prices.append(price.text)
        
        # Собираем продолжительность
        duration = course.find('div', class_='l-dates flex items-baseline no-wrap')
        if duration is not None:
            durations.append(duration.find('div').text)
        else:
            durations.append(np.nan)
            
        # Собираем названия компаний
        try:
            company = course.find('div', class_="q-img q-img--menu")['aria-label']
            companies.append(company)
        except:
            companies.append(np.nan)

        # Добавляем категорию
        labels.append(label[i])

# Создаем датафрейм
df = pd.DataFrame()
df['Name'] = names
df['Rate'] = rates
df['Price'] = prices
df['Duration'] = durations
df['Company'] = companies
df['Label'] = labels

# Записываем датафрейм в csv
df.to_csv('courses_.csv', sep='|')
