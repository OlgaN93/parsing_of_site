import config
import telebot
import metods
import psycopg2
import constants

from time import sleep

bot = telebot.TeleBot(config.token)

# def init_db(db, tbl, connection):
#     get_ad.create_db(db)
#     cursor = connection.cursor()
#     get_ad.create_table(connection, cursor, tbl)
#     return cursor

def fill_table(cur_fill):
    soup_first_p = metods.create_soup(constants.first_page)
    table = soup_first_p.find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
    for tr in table.tr.next_siblings:
        try:
            info = metods.pars_info(tr)
            metods.save_info(info, cur_fill)
        except UnboundLocalError:
            # ОШИБКА: local variable 'record' referenced before assignment
            continue

    font = soup_first_p.find('font', class_='text').next_sibling
    for sibling in font.b.next_siblings:
        # Необходимо пройтись по всем страницам объявлений. Для этого достаточно пройтись по всем ссылкам с числами,
        # игнорируя ссылки с текстом ("пред.","след." и т.д).
        if sibling.text.isdigit():
            href = sibling.get('href')
            need_url = config.base_url + href
            soup_need_p = metods.create_soup(need_url)
            table = soup_need_p.find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
            for tr in table.tr.next_siblings:
                try:
                    info = metods.pars_info(tr)
                    metods.save_info(info, cur_fill)
                except UnboundLocalError:
                    #ОШИБКА: local variable 'record' referenced before assignment
                    continue


@bot.message_handler(commands=['start'])
def new_ads(message):
    while True:
        try:
            conn_new = metods.connection_db(db_name)
            cur_new = conn_new.cursor()
            soup_p = metods.create_soup(constants.first_page)
            table = soup_p.find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
            for tr in table.tr.next_siblings:
                try:
                    info = metods.pars_info(tr)
                    link = metods.save_info(info, cur_new)
                    bot.send_message(message.chat.id, link)
                except UnboundLocalError:
                    #ОШИБКА: local variable 'record' referenced before assignment
                    continue
        except psycopg2.errors.UniqueViolation:
            # ОШИБКА: повторяющееся значение ключа нарушает ограничение уникальности
            pass
        except (Exception, psycopg2.Error) as error:
            print('Ошибка при работе с PostgreSQL', error)
        finally:
            metods.close_db(conn_new, cur_new)

        print('Цикл завершён')
        sleep(config.period_check_data)


db_name = config.db_name
try:
    metods.create_db(db_name)
    connection = metods.connection_db(db_name)
    cursor = connection.cursor()
    metods.create_table(connection, cursor)
#    cursor = init_db(db_name, tbl_name)
    fill_table(cursor)
except psycopg2.errors.UniqueViolation:
    # ОШИБКА: повторяющееся значение ключа нарушает ограничение уникальности
    pass
except (Exception, psycopg2.Error) as error:
    print('Ошибка при работе с PostgreSQL', error)
finally:
    metods.close_db(connection, cursor)



#TODO: метод для инициализации базы данных, метод для заполнения базы данных, вынести все из старта
#TODO: константы в отдельный класс
#TODO: метод для настройки period_check_data каждым пользователем
#TODO: изменить current page на другое название
#TODO: разобраться с полингом


if __name__ == '__main__':
    bot.polling(none_stop=True, interval=0)
