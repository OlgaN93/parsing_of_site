import re
import bs4
import config


def get_info(tr: bs4.element.Tag):
    """Возвращает данные по объекту недвижимости на основе тега с объявлением

    :param tr: содержит объявление.
    :type tr: bs4.element.Tag
    :return: содержит цифровую часть url-адреса, url-адрес, адрес, цену, этаж и
            площадь объекта недвижимости.
    :rtype: tuple
    """

    td_with_address = tr.find('td', class_=False)
    address = td_with_address.a
    ad_href = address.get('href')
    url = config.domain + ad_href
    url_id = re.search(r'\d+', ad_href)[0]
    address = str(address)
    price = str(td_with_address.next_sibling.string)
    floor = str(td_with_address.next_sibling.next_sibling.string)
    square = str(td_with_address.next_sibling.next_sibling.next_sibling.next_sibling.string)
    info = (url_id, url, address, price, floor, square)
    return info

