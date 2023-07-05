import telebot
import psycopg2
import sys
import metods
import config
import constants

from time import sleep

bot = telebot.TeleBot(config.token)

first_page = config.domain + constants.path_to_page + constants.params_to_page

try:
    metods.create_db(config.db_password, config.db_ads)
    connection = metods.connection_db(config.db_password, config.db_ads)
    cursor = connection.cursor()
    metods.create_ads_table(connection, cursor)
    soup_first_p = metods.create_soup(first_page)
    metods.fill_table(connection, cursor, first_page)
except (Exception, psycopg2.Error) as error:
    print('Ошибка при работе с PostgreSQL', error)
    sys.exit()
finally:
    metods.close_db(connection, cursor)


@bot.message_handler(commands=['start'])
def new_ads(message):
    while True:
        try:
            #TODO: Убрать new
            conn = metods.connection_db(config.db_password, config.db_ads)
            cur = conn.cursor()
            soup_p = metods.create_soup(first_page)
            table = soup_p.find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
            for tr in table.tr.next_siblings:
                try:
                    info = metods.get_info(tr)
                    link = metods.save_info(info, cur)
                    if link:
                        bot.send_message(message.chat.id, link)
                except psycopg2.errors.UniqueViolation:
                    # ОШИБКА: повторяющееся значение ключа нарушает ограничение уникальности
                    continue
                finally:
                    conn.commit()
        except (Exception, psycopg2.Error) as error:
            print('Ошибка при работе с PostgreSQL', error)
        finally:
            metods.close_db(conn, cur)

        print('Цикл завершён')
        sleep(config.period_check_data_cek)

#TODO: Разобраться с полингом
if __name__ == '__main__':
    bot.polling(none_stop=True, interval=0)