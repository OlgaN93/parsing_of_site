import psycopg2
import requests
import re
import config
import constants

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from bs4 import BeautifulSoup


def create_db(db_password, db_ads):
    r""" Создает базу данных

        :param db_password: :class: str, пароль от базы данных.
        :param db_ads: :class: str, название базы данных.
    """

    conn_main = psycopg2.connect(user='postgres',
                                 password=db_password,
                                 host='localhost',
                                 port='5432')
    conn_main.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur_main = conn_main.cursor()
    cur_main.execute(f"SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = '{db_ads}')")
    answer = cur_main.fetchone()[0]
    if not answer:
        sql_create_database = f'create database {db_ads};'
        cur_main.execute(sql_create_database)
        print(f"База данных {db_ads} успешно создана")


def connection_db(db_password, db_ads):

    r""" Создает соединение с базой данных

        :param db_password: :class: str, пароль от базы данных.
        :param db_ads: :class: str, название базы данных.
        :return: :class: psycopg2.extensions.connection, управляет подключением к экземпляру базы данных PostgreSQL.
        """

    connection = psycopg2.connect(user='postgres',
                                  password=db_password,
                                  host='localhost',
                                  port='5432',
                                  database=db_ads)
    return connection


#TODO: Реализовать универсальный метод (передача названия тадлицы, названия колонок, типа значения, типа ключа)
def create_ads_table(connection, cursor):

    r""" Создает таблицу с информацией из объявлений

        :param connection: :class: psycopg2.extensions.connection, управляет подключением к экземпляру базы данных PostgreSQL.
        :param cursor: psycopg2.extensions.cursor, предоставляет методы для выполнения команд PostgreSQL.
        """

    cursor.execute(f"SELECT EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{constants.tbl_name}')")
    answer = cursor.fetchone()[0]
    if not answer:
        create_tbl = f'''CREATE TABLE {constants.tbl_name}
                                (ID             integer PRIMARY KEY, 
                                LINK            varchar,
                                ADDRESS         varchar,
                                PRICE           varchar,
                                FLOOR           varchar,
                                SQUARE          varchar);'''
        cursor.execute(create_tbl)
        connection.commit()
        print('Таблица успешно создана в PostgreSQL')



def create_soup(url):

    r""" Получает и парсит страницу по переданному URL-адресу

        :param url: :class: str, адрес страницы.
        :return: :class:`BeautifulSoup` object.
        """

    response = requests.get(url).text
    res_text = re.sub(r'>\s+<', '><', response.replace('\n', ''))
    soup = BeautifulSoup(markup=res_text, features="html.parser")
    return soup


#TODO: Подумать над типом возвращаемого значения
def get_info(tr):

    r""" Возвращает данные по объекту недвижимости на основе тега с объявлением

        :param tr: :class: tag, содержащий объявление.
        :return: :class: tuple, содержащий цифровую часть url-адреса, url-адрес, адрес, цену, этаж и
                площадь объекта недвижимости.
        """

    td_with_address = tr.find('td', class_=False)
    address = td_with_address.a
    url = config.domain + address.get('href')
    url_id = re.search(r'\d+', address.get('href'))[0]
    address = str(address)
    price = str(td_with_address.next_sibling.string)
    floor = str(td_with_address.next_sibling.next_sibling.string)
    square = str(td_with_address.next_sibling.next_sibling.next_sibling.next_sibling.string)
    info = (url_id, url, address, price, floor, square)
    return info


#TODO: Как можно написать униваерсально?
def save_info(info, cursor):

    r""" Сохраняет информацию в базу данных

        :param info: :class: tuple, содержащий цифровую часть url-адреса, url-адрес, адрес, цену, этаж и
                площадь объекта недвижимости.
        :param cursor: :class: psycopg2.extensions.cursor, предоставляет методы для выполнения команд PostgreSQL.
        :return: :class: str, ссылка на новое объявление.
        """

    insert_query = f"INSERT INTO {constants.tbl_name} (ID, LINK, ADDRESS, PRICE, FLOOR, SQUARE)" \
                   f"VALUES (%s, %s, %s, %s, %s, %s)"

    cursor.execute(insert_query, info)
    record = info[1]
    print(record)

    return record


def fill_table(conn_db, cur_fill, first_p):
    soup_first_p = create_soup(first_p)
    end = soup_first_p.find('a', string='Конец')
    num_page = int(re.search(r'.*PAGEN_1=(\d+)">Конец</a>', str(end)).group(1))

    while num_page:
        need_page = f"{first_p}&PAGEN_1={num_page}"
        soup_need_p = create_soup(need_page)
        table = soup_need_p.find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
        for tr in table.tr.next_siblings:
            try:
                info = get_info(tr)
                save_info(info, cur_fill)
            except psycopg2.errors.UniqueViolation:
                # ОШИБКА: повторяющееся значение ключа нарушает ограничение уникальности
                continue
            finally:
                conn_db.commit()
        num_page = num_page - 1

def close_db(connection, cursor):

    r""" Закрывает соединение с базой данных

        :param connection: :class: psycopg2.extensions.connection, управляет подключением к экземпляру базы данных PostgreSQL.
        :param cursor: psycopg2.extensions.cursor, предоставляет методы для выполнения команд PostgreSQL.
        """

    if connection:
        cursor.close()
        connection.close()
