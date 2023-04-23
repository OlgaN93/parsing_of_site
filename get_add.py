import hashlib
import requests
import re

from bs4 import BeautifulSoup


def create_soup(url):
    r""" Получает и парсит страницу по переданному URL-адресу

        :param url: :class: str.
        :return: :class:`BeautifulSoup` object
        """

    response = requests.get(url).text
    res_text = re.sub(r'>\s+<', '><', response.replace('\n', ''))
    soup = BeautifulSoup(markup=res_text, features="html.parser")
    return soup


def get_info(table, base_url, set_hash, list_hash_url, infa):
    r""" Получает информацию со страницы, определяет ее новизну, отправляет новую информацию в файл и телеграмм-боту,
    а также выводит на экран (Уместить в одну строчку)

        :param table: :class: teg, содержит объявления.
        :param base_url: :class: str.
        :param set_hash: :class: set, множество хешей url-адресов.
        :param list_hash_url: :class: file, хранит с прошлых  запусков программы.
        :param infa: :class: file, хранит информацию из объявлений.
        :return: :class: set, содержит уникальные url-адреса объявлений.
        """

    urls = set()
    for tr in table.tr.next_siblings:
        td_with_address = tr.find('td', class_=False)
        tag_a = td_with_address.a
        url = base_url + tag_a.get('href')
        url_hash = hashlib.md5(url.encode()).hexdigest()
        if url_hash not in set_hash:
            set_hash.add(url_hash)
            urls.add(url)
            print(url_hash, file=list_hash_url)
            price = td_with_address.next_sibling.string
            floor = td_with_address.next_sibling.next_sibling.string
            print(f"""Ссылка: {url}
    Адрес: {tag_a.string}
    Цена: {price}
    Этаж: {floor if floor is not None else 'не указан'}
    """, file=infa)
            result = f"""Ссылка: {url}
    Адрес: {tag_a.string}
    Цена: {price}
    Этаж: {floor if floor is not None else 'не указан'}
    """
            print(result)
    return urls
