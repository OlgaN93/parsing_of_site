import telebot
import psycopg2
import metods
import config
import constants
import Page
import Storage

from time import sleep

bot = telebot.TeleBot(config.token)

base_url = config.domain + constants.path_to_page + constants.params_to_page
first_page = Page.Page(url=base_url)
three_room = constants.tbl_name

sale_db = Storage.DataBase(config.db_ads, config.db_password)

try:
    sale_db.create_ads_table(three_room)
    sale_db.fill_table(first_page, three_room)
except (Exception, psycopg2.Error) as error:
    raise error('Ошибка при работе с PostgreSQL')
finally:
    sale_db.connection.close()

@bot.message_handler(commands=['start'])
def new_ads(message):
    sale_db.connection_db()
    while True:
        try:
            for table_row in first_page.table_rows():
                try:
                    info = metods.get_info(table_row)
                    link = sale_db.save_info(three_room, info)
                    if link:
                        bot.send_message(message.chat.id, link)
                except psycopg2.errors.UniqueViolation:
                    # ОШИБКА: повторяющееся значение ключа нарушает ограничение уникальности
                    continue
        except (Exception, psycopg2.Error) as error:
            print('Ошибка при работе с PostgreSQL', error)

        print('Цикл завершён')
        sleep(config.period_check_data_cek)

#TODO: Разобраться с полингом
if __name__ == '__main__':
    bot.polling(none_stop=True, interval=0)
