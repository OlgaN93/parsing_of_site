import requests
import re
import psycopg2

from bs4 import BeautifulSoup
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import your_password


def create_soup(url):

    r""" Получает и парсит страницу по переданному URL-адресу

        :param url: :class: str.
        :return: :class:`BeautifulSoup` object
        """

    response = requests.get(url).text
    res_text = re.sub(r'>\s+<', '><', response.replace('\n', ''))
    soup = BeautifulSoup(markup=res_text, features="html.parser")
    return soup


def pars_info(tr, base_url):

    r""" Анализирует таблицу с объявлениями

        :param tr: :class: teg, содержит объявления.
        :param base_url: :class: str.
        :return: :class: str, результат функции send_info.
        """

    td_with_address = tr.find('td', class_=False)
    address = td_with_address.a
    url = base_url + address.get('href')
    url_id = re.search(r'\d+', address.get('href'))[0]
    address = str(address)
    price = str(td_with_address.next_sibling.string)
    floor = str(td_with_address.next_sibling.next_sibling.string)
    square = str(td_with_address.next_sibling.next_sibling.next_sibling.next_sibling.string)
    info = (url_id, url, address, price, floor, square)
    return info

def save_info(info, cursor, tbl):

    r""" Сохраняет информацию из объявлений в базу данных

        :param info: :class: tuple, .
        :param cursor: psycopg2.extensions.cursor.
        :param tbl: :class: str, название таблицы.
        :return: record :class: str, ссылка на новое объявление.
        """

    insert_query = f"INSERT INTO {tbl} (ID, ССЫЛКА, АДРЕС, ЦЕНА, ЭТАЖ, ПЛОЩАДЬ) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(insert_query, info)
    cursor.execute(f"SELECT * from {tbl} ORDER BY ID DESC LIMIT 1")
    record = cursor.fetchone()[1]
    print(record)

    return record

def create_bd(bd):

    r""" Создает базу данных

        :param bd: :class: str, название базы данных.
        """
    
    conn_main = psycopg2.connect(user='postgres',
                                 password=your_password,
                                 host='localhost',
                                 port='5432')
    conn_main.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur_main = conn_main.cursor()
    cur_main.execute(f"SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = '{bd}')")
    answer = cur_main.fetchone()[0]
    if not answer:
        sql_create_database = f'create database {bd};'
        cur_main.execute(sql_create_database)
        print(f"База данных {bd} успешно создана")


def create_table(conn_tabl, cur_table, tbl):

    r""" Создает таблицу с информацией из объявлений

        :param conn_tabl: :class: psycopg2.extensions.connection.
        :param cur_table: psycopg2.extensions.cursor.
        :param tbl: :class: str, название таблицы.
        """

    cur_table.execute(f"SELECT EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{tbl}')")
    answer = cur_table.fetchone()[0]
    if not answer:
        create_table = f'''CREATE TABLE {tbl}
                            (ID         integer PRIMARY KEY, 
                            ССЫЛКА      varchar,
                            АДРЕС       varchar,
                            ЦЕНА        varchar,
                            ЭТАЖ        varchar,
                            ПЛОЩАДЬ     varchar);'''
        cur_table.execute(create_table)
        conn_tabl.commit()
        print('Таблица успешно создана в PostgreSQL')


def connection_bd(bd):

    r""" Создает соединение с базой данных

        :param bd: :class: str, название базы данных.
        :return: :class: str, ссылка на новое объявление.
        """

    connection = psycopg2.connect(user='postgres',
                                  password=your_password,
                                  host='localhost',
                                  port='5432',
                                  database=bd)
    return connection


def close_bd(conn_bd, cur_bd):

    r""" Закрывает соединение с базой данных

        :param conn_bd: :class: psycopg2.extensions.connection.
        :param cur_bd: psycopg2.extensions.cursor.
        """

    conn_bd.commit()
    if conn_bd:
        cur_bd.close()
        conn_bd.close()
