import config
import telebot
import get_ad
import psycopg2

from time import sleep
from psycopg2 import Error

bot = telebot.TeleBot(config.token)

@bot.message_handler(commands=['start'])
def start(message):
    bd = 'sale_of_apartments'
    tbl = 'three_room'
    base_url = config.base_url
    sids = {'three_room': '4'}
    var = {'sale': '313'}
    first_page = base_url + '/re/search.php?sid=' + sids['three_room'] + '&var=' + var['sale']
    soup_first_p = get_ad.create_soup(first_page)
    font = soup_first_p.find('font', class_='text').next_sibling
    try:
        get_ad.create_bd(bd)
        connection = get_ad.connection_bd(bd)
        cursor = connection.cursor()
        get_ad.create_table(connection, cursor, tbl)

        for sibling in font.b.next_siblings:
            if sibling.text.isdigit():
                href = sibling.get('href')
                curr_url = base_url + href
                soup_curr_p = get_ad.create_soup(curr_url)
                table = soup_curr_p.find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
                for tr in table.tr.next_siblings:
                    try:
                        info = get_ad.pars_info(tr, base_url)
                        link = get_ad.save_info(info, cursor, tbl)
                        bot.send_message(message.chat.id, link)
                        sleep(1)
                    except UnboundLocalError:
                        #ОШИБКА: local variable 'record' referenced before assignment
                        continue
    except psycopg2.errors.UniqueViolation:
        #ОШИБКА: повторяющееся значение ключа нарушает ограничение уникальности
        pass
    except (Exception, Error) as error:
        print('Ошибка при работе с PostgreSQL', error)
    finally:
        get_ad.close_bd(connection, cursor)


    while True:
        try:
            connection = get_ad.connection_bd(bd)
            cursor = connection.cursor()
            soup_first_p = get_ad.create_soup(first_page)
            table = soup_first_p.find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
            for tr in table.tr.next_siblings:
                try:
                    info = get_ad.pars_info(tr, base_url)
                    link = get_ad.save_info(info, cursor, tbl)
                    bot.send_message(message.chat.id, link)
                except UnboundLocalError:
                    #ОШИБКА: local variable 'record' referenced before assignment
                    continue
        except psycopg2.errors.UniqueViolation:
            # ОШИБКА: повторяющееся значение ключа нарушает ограничение уникальности
            pass
        except (Exception, Error) as error:
            print('Ошибка при работе с PostgreSQL', error)
        finally:
            get_ad.close_bd(connection, cursor)

        print('Цикл завершён')
        sleep(5)


if __name__ == '__main__':
#TODO: проверка существования таблицы
    bot.polling(none_stop=True, interval=0)
