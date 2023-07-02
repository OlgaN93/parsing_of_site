import constants
import requests
import re
import psycopg2
import config

from bs4 import BeautifulSoup
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT



def create_soup(url):

    r""" Получает и парсит страницу по переданному URL-адресу

        :param url: :class: str, адрес страницы с объявлениями.
        :return: :class:`BeautifulSoup` object.
        """

    response = requests.get(url).text
    res_text = re.sub(r'>\s+<', '><', response.replace('\n', ''))
    soup = BeautifulSoup(markup=res_text, features="html.parser")
    return soup


def pars_info(tr):

    r""" Возвращает данные по объекту недвижимости на основе тега с объявлением

        :param tr: :class: tag, содержащий объявление.
        :param base_url: :class: str, адрес главной страницы сайта.
        :return: :class: tuple, содержащий цифровую часть url-адреса, url-адрес, адрес, цену, этаж и площадь объекта недвижимости.
        """

    td_with_address = tr.find('td', class_=False)
    address = td_with_address.a
    url = config.base_url + address.get('href')
    url_id = re.search(r'\d+', address.get('href'))[0]
    address = str(address)
    price = str(td_with_address.next_sibling.string)
    floor = str(td_with_address.next_sibling.next_sibling.string)
    square = str(td_with_address.next_sibling.next_sibling.next_sibling.next_sibling.string)
    info = (url_id, url, address, price, floor, square)
    return info

def save_info(info, cursor):

    r""" Сохраняет информацию в базу данных

        :param info: :class: tuple, содержащий цифровую часть url-адреса, url-адрес, адрес, цену, этаж и площадь объекта недвижимости.
        :param cursor: :class: psycopg2.extensions.cursor, предоставляет методы для выполнения команд PostgreSQL.
        :return: :class: str, ссылка на новое объявление.
        """
    tbl = constants.tbl_name
    insert_query = f"INSERT INTO {tbl} (ID, LINK, ADDRESS, PRICE, FLOOR, SQUARE) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(insert_query, info)
    record = info[1]
    print(record)

    return record

def create_db(db_name):

    r""" Создает базу данных

        :param db_name: :class: str, название базы данных.
        """
    
    conn_main = psycopg2.connect(user='postgres',
                                 password=config.db_password,
                                 host='localhost',
                                 port='5432')
    conn_main.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur_main = conn_main.cursor()
    cur_main.execute(f"SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = '{db_name}')")
    answer = cur_main.fetchone()[0]
    if not answer:
        sql_create_database = f'create database {db_name};'
        cur_main.execute(sql_create_database)
        print(f"База данных {db_name} успешно создана")


def create_table(conn_tabl, cur_table):

    r""" Создает таблицу с информацией из объявлений

        :param conn_tabl: :class: psycopg2.extensions.connection.
        :param cur_table: psycopg2.extensions.cursor.
        """
    tbl = constants.tbl_name
    cur_table.execute(f"SELECT EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{tbl}')")
    answer = cur_table.fetchone()[0]
    if not answer:
        create_tbl = f'''CREATE TABLE {tbl}
                            (ID             integer PRIMARY KEY, 
                            LINK            varchar,
                            ADDRESS         varchar,
                            PRICE           varchar,
                            FLOOR           varchar,
                            SQUARE          varchar);'''
        cur_table.execute(create_tbl)
        conn_tabl.commit()
        print('Таблица успешно создана в PostgreSQL')


def connection_db(db):

    r""" Создает соединение с базой данных

        :param db: :class: str, название базы данных.
        :return: :class: str, ссылка на новое объявление.
        """

    connection = psycopg2.connect(user='postgres',
                                  password=config.db_password,
                                  host='localhost',
                                  port='5432',
                                  database=db)
    return connection


def close_db(conn_db, cur_db):

    r""" Закрывает соединение с базой данных

        :param conn_db: :class: psycopg2.extensions.connection.
        :param cur_db: psycopg2.extensions.cursor.
        """

    conn_db.commit()
    if conn_db:
        cur_db.close()
        conn_db.close()
