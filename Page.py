import re
import requests

from bs4 import BeautifulSoup

class Page:
    """Класс, используемый для представления базы данных

    :param url: URL-адрес страницы
    :type url: str
    """
    def __init__(self, url: str):
        self.url = url

    def create_soup(self):
        """Получает и парсит страницу по переданному URL-адресу

            :return: страница в виде вложенной структуры данных
            :rtype:`BeautifulSoup` object.
            """

        response = requests.get(self.url).text
        res_text = re.sub(r'>\s+<', '><', response.replace('\n', ''))
        soup = BeautifulSoup(markup=res_text, features="html.parser")
        return soup

    def qty_of_page(self):
        """Считает количество страниц с объявлениями

        :return: количество страниц с объявлениями
        :rtype: int
        """
        end = self.create_soup().find('a', string='Конец')
        num_page = int(re.search(r'.*PAGEN_1=(\d+)">Конец</a>', str(end)).group(1))
        return num_page

    def table_rows(self):
        """Находит строки с объявлениями

        :return: строки с объявлениями
        :rtype: generator
        """
        table = self.create_soup().find(lambda tag: tag.name == 'table' and tag.get('class') == ['car_list'])
        return table.tr.next_siblings

