import requests
from bs4 import BeautifulSoup
import math
import pandas as pd
import concurrent.futures

URL = 'https://zakup.nationalbank.kz/ru/publics/buys'
# для имитации работы браузера
HEADERS = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36', 'accept': '*/*'}
HOST = 'https://zakup.nationalbank.kz'
# FILE = 'tenders.csv'
dataset = pd.DataFrame(columns=["Название","Дата начала","Дата окончания","Статус организатора","Банковские реквизиты"])
#get request
def get_html(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    return r

# находим реквезит по индексу     
def get_requisites(link):
    html = get_html(link)
    if html.status_code == 200:
        soup = BeautifulSoup(html.text, 'html.parser')
        rec = soup.find("table", {"id": "w0"}).find_all('td')[6].text.replace('\n',
                        ', ').replace('город','г.').replace('район',
                        'р.').replace('Проспект','п.').replace('здание','зд.').replace('Название банка','Банк')
        return rec
    else:
        print('Error')
#берем общее количество элементов и делим на кол. на одной странице и округляем 
def get_pages_count(html):
    soup = BeautifulSoup(html, 'html.parser')
    perpage = soup.find('div', class_='summary').find_all('b')[0].text.replace('1-','')
    amount =  soup.find('div', class_='summary').find_all('b')[1].text.replace(u'\xa0','')
    pages=math.ceil(int(amount)/int(perpage))
    return pages

def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('tr', class_='item')
    tenders = []
    i=0
    for item in items:
        td_data=item.find_all('td', class_="hidden-xs")
#элементы с одинаковыми классами записываем в лист
        values = [ele.text.strip() for ele in td_data]
        keys=['method','b_date','e_date','organizer','status']
#можно было просто по индексу взять но я записала в словарь
        td=dict(zip(keys, values))
        link=item.find('a', class_='word-break').get('href')
        rec=get_requisites(link)
        title = item.find('a', class_='word-break').get_text()
        b_date =td['b_date']
        e_date =td['e_date']
        status =td['status']
        requisites= rec
        dataset.loc[len(dataset)] = [title, b_date, e_date, status, requisites]
    print(f"Thread finished")
              
# def parse():
#     html = get_html(URL)
#     if html.status_code == 200:
# #         pages=2
#         pages=get_pages_count(html.text)
#         for page in range(1, pages):
#             print(f'Парсинг страницы {page} из {pages}...')
#             html = get_html(URL, params={'page': page})
#             get_content(html.text)
#     else:
#         print('Error')



no_threads = 5
def multi_parse():
    html = get_html(URL)
    if html.status_code == 200:
        pages=get_pages_count(html.text)
        with concurrent.futures.ThreadPoolExecutor(max_workers=no_threads) as executor:
            try:
                for page in range(1, pages+1):
                    print(f"Thread starting for page: {page}")
                    html = get_html(URL, params={'page': page})
                    executor.submit(get_content,html.text)
            except concurrent.futures.TimeoutError:
                get_content.interrupt()
    else:
        print('Error')



multi_parse()
# parse()

dataset.to_csv('tenders.csv',encoding='utf-8-sig', sep=';',index = False)

