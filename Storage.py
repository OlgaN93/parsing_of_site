import psycopg2
import Page
import metods

from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DataBase:
    """Класс, используемый для представления базы данных

    :param name: название базы данных
    :type name: str
    :param password: пароль для доступа к базе данных
    :type password: str
    :param connection: управляет подключением к экземпляру базы данных PostgreSQL
    :type connection: psycopg2.extensions.connection
    """

    def __init__(self, name: str, password: str):
        self.name = name
        self.password = password
        self.create_db()
        self.connection = self.connection_db()

    def create_db(self):
        """Создает базу данных """

        try:
            connection = psycopg2.connect(user='postgres',
                                          password=self.password,
                                          host='localhost',
                                          port='5432')

            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with connection.cursor() as curs:
                curs.execute(
                    f"SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = '{self.name}')")
                answer = curs.fetchone()[0]
                if not answer:
                    sql_create_database = f'create database {self.name};'
                    curs.execute(sql_create_database)
                    print(f"База данных {self.name} успешно создана")
        except(Exception, psycopg2.Error) as error:
            raise error('Ошибка при работе с PostgreSQL')
        finally:
            connection.close()

    def connection_db(self):
        """ Создает соединение с базой данных

        :return: управляет подключением к экземпляру базы данных PostgreSQL
        :rtype: psycopg2.extensions.connection
        """

        self.connection = psycopg2.connect(user='postgres',
                                      password=self.password,
                                      host='localhost',
                                      port='5432',
                                      database=self.name)

        self.connection.set_isolation_level(ISOLATION_LEVEL_REPEATABLE_READ)

        return self.connection

    def create_ads_table(self, tabl_name: str):
        """Создает таблицу с информацией из объявлений

        :param tabl_name: название таблицы с объявлениями
        :type: str
        """

        self.connection.set_isolation_level(ISOLATION_LEVEL_REPEATABLE_READ)
        with self.connection:
            with self.connection.cursor() as curs:
                curs.execute(
                    f"SELECT EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{tabl_name}')")
                answer = curs.fetchone()[0]
                if not answer:
                    create_tbl = f'''CREATE TABLE {tabl_name}
                                            (ID             integer PRIMARY KEY, 
                                            LINK            varchar,
                                            ADDRESS         varchar,
                                            PRICE           varchar,
                                            FLOOR           varchar,
                                            SQUARE          varchar);'''
                    curs.execute(create_tbl)

        with self.connection:
            with self.connection.cursor() as curs:
                curs.execute(
                    f"SELECT EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{tabl_name}')")
                answer = curs.fetchone()[0]
                if answer:
                    print('Таблица успешно создана в PostgreSQL')

    def save_info(self, tabl_name: str, info: tuple):
        """Сохраняет информацию в базу данных

        :param tabl_name: название таблицы с объявлениями
        :type tabl_name: str
        :param info: содержащий цифровую часть url-адреса, url-адрес, адрес, цену, этаж и площадь объекта
                недвижимости
        :type info: tuple
        :return: ссылка на новое объявление
        :rtype: str
        """

        insert_query = f"INSERT INTO {tabl_name} (ID, LINK, ADDRESS, PRICE, FLOOR, SQUARE)" \
                       f"VALUES (%s, %s, %s, %s, %s, %s)"
        with self.connection:
            with self.connection.cursor() as curs:
                curs.execute(insert_query, info)
                record = info[1]
                print(record)

        return record

    def fill_table(self, first_page: Page, tabl_name: str):
        """Заполняет таблицу существующими объявлениями

        :param first_page: первая страница с обявлениями
        :type first_page: Page
        :param tabl_name: название таблицы с объявлениями
        :type tabl_name: str
        """

        qty_of_page = first_page.qty_of_page()

        for num_page in range(1, qty_of_page + 1):
            page = Page.Page(url=f"{first_page.url}&PAGEN_1={num_page}")

            for table_row in page.table_rows():
                try:
                    info = metods.get_info(table_row)
                    self.save_info(tabl_name, info)
                except psycopg2.errors.UniqueViolation:
                    # ОШИБКА: повторяющееся значение ключа нарушает ограничение уникальности
                    continue

    def drop_db(self):
        """Удаляет базу данных"""

        connection = psycopg2.connect(user='postgres',
                                     password=self.password,
                                     host='localhost',
                                     port='5432')
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = connection.cursor()
        cur.execute(f"DROP database {self.name}")
        cur.execute(f"SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = '{self.name}')")
        answer = cur.fetchone()[0]
        if not answer:
            print(f"База данных {self.name} успешно удалена")
        connection.close()
