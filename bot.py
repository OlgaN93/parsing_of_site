import config
import telebot
import get_add

from time import sleep

bot = telebot.TeleBot(config.token)


@bot.message_handler(commands=['start'])
def start(message):
    set_hash = set()
    base_url = config.base_url
    sids = {'three_room': '4'}
    var = {'sale': '313'}

    with open('list_hash_url.txt', 'a+') as list_hash_url, open('infa.txt', 'a+', encoding='utf-8') as infa:
        list_hash_url.seek(0)
        for line in list_hash_url:
            line = line.replace('\n', '')
            set_hash.add(line)

        first_page = base_url + '/re/search.php?sid=' + sids['three_room'] + '&var=' + var['sale']
        soup_first_p = get_add.create_soup(first_page)
        font = soup_first_p.find('font', class_='text').next_sibling

        for sibling in font.b.next_siblings:
            if sibling.text.isdigit():
                href = sibling.get('href')
                curr_url = base_url + href
                soup_curr_p = get_add.create_soup(curr_url)
                table = soup_curr_p.find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
                links = get_add.get_info(table, base_url, set_hash, list_hash_url, infa)
                for link in links:
                    bot.send_message(message.chat.id, link)
                    sleep(1)

        while True:
            soup_first_p = get_add.create_soup(first_page)
            table = soup_first_p.find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
            links = get_add.get_info(table, base_url, set_hash, list_hash_url, infa)
            for link in links:
                bot.send_message(message.chat.id, link)

            list_hash_url.flush()
            infa.flush()

            print('Цикл завершён')
            sleep(5)


if __name__ == '__main__':
    bot.infinity_polling()
