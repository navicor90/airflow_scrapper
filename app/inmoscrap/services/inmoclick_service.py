from app.inmoscrap.models import PropertyType
from urllib.parse import urlparse
from datetime import datetime
import locale

# $ sudo locale-gen es_ES
# $ sudo locale-gen es_ES.UTF-8
locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')

inmoclick_property_map = {PropertyType.LAND: "lotes-y-terrenos",
                          PropertyType.HOUSE: "casas",
                          PropertyType.APARTMENT: "departamentos"}


def to_inmoclick_property_type(property_type: PropertyType):
    try:
        return inmoclick_property_map[property_type]
    except:
        raise Exception(f"Property type {property_type} not supported yet")


def search_url(property_type: PropertyType, page:int):
    page = str(page)
    base_url = 'https://www.inmoclick.com.ar'
    inmoclick_type = to_inmoclick_property_type(property_type)

    params = "?favoritos=0&limit=48&prevEstadoMap=&provincias=21&precio%5Bmin%5D=&precio%5Bmax%5D=&moneda=1&sup_cubierta%5Bmin%5D=&sup_cubierta%5Bmax%5D=&sup_total%5Bmin%5D=&sup_total%5Bmax%5D=&page="
    url = f'{base_url}/inmuebles/venta/{inmoclick_type}/mendoza{params}{page}'
    return url


class SearchItem(object):
    html_article = None
    property_type = None

    def __init__(self, html_article, property_type: PropertyType):
        self.html_article = html_article
        self.property_type = property_type

    def ref_id(self):
        return self.html_article.a.get("name").strip()

    def district(self):
        return self.html_article.find('span', attrs={'itemprop': 'addressLocality'}).text.strip()

    def neighborhood(self):
        return self.html_article.find('p', attrs={'itemprop': 'streetAddress'}).text.strip()

    def province(self):
        return self.html_article.find('span', attrs={'itemprop': 'addressRegion'}).text.strip()

    def price_dict(self):
        price = {}
        price['price'] = self.html_article.get("precio").strip()
        if not 'consultar' in price['price'].lower():
            price['currency'] = price['price'].strip().split(' ')[0]
            price['amount'] = locale.atof(price['price'].strip().split(' ')[1])
        return price

    def description(self):
        return (self.html_article.find('div', attrs={'class': 'description-hover'})
                .p.text).strip()

    def article_link(self):
        return (self.html_article.find('div', attrs={'class': 'description-hover'})
                .a.next_sibling.get('href')).strip()

    def total_area(self):
        return self.html_article.get("sup_t").strip()

    def covered_area(self):
        return self.html_article.get("sup_c").strip()

    def bedrooms(self):
        bedrooms = "empty"
        span = self.html_article.find('span', attrs={'class': 'label-dormitorio'})
        if span:
            bedrooms = span.text.strip()
        return bedrooms

    def bathrooms(self):
        bathrooms = "empty"
        span = self.html_article.find('span', attrs={'class': 'label-banio'})
        if span:
            bathrooms = span.text.strip()
        return bathrooms

    def has_gas(self):
        div = self.html_article.find('div', attrs={'class': 'icon-gas'})
        if not div:
            return "empty"
        classes = div.get('class')
        if 'disable' in classes:
            return False
        return True

    def has_water(self):
        div = self.html_article.find('div', attrs={'class': 'icon-agua'})
        if not div:
            return "empty"
        classes = div.get('class')
        if 'disable' in classes:
            return False
        return True

    def has_electricity(self):
        div = self.html_article.find('div', attrs={'class': 'icon-luz'})
        if not div:
            return "empty"
        classes = div.get('class')
        if 'disable' in classes:
            return False
        return True

    def agency(self):
        agency = None
        owner_div = self.html_article.find('div', attrs={'class': 'property-brand'})
        if owner_div.img:
            agency = owner_div.img.get('title')
        else:
            agency = owner_div.p.text
        return agency.strip()

    def to_dict(self):
        property_dict = {}

        property_dict['ref_id'] = self.ref_id()

        # Property data
        property_dict['neighborhood'] = self.neighborhood()
        property_dict['district'] = self.district()
        property_dict['province'] = self.province()
        property_dict.update(self.price_dict())
        property_dict['url'] = self.article_link()
        property_dict['description'] = self.description()
        property_dict['totalArea'] = self.total_area()

        if self.property_type.value in [PropertyType.HOUSE.value, PropertyType.APARTMENT.value]:
            # Property tags
            property_dict['bedrooms'] = self.bedrooms()
            property_dict['bathrooms'] = self.bathrooms()
            property_dict['floorArea'] = self.covered_area()

        if self.property_type.value in [PropertyType.LAND.value]:
            # Property tags
            property_dict['has_water'] = self.has_water()
            property_dict['has_electricity'] = self.has_electricity()
            property_dict['has_gas'] = self.has_gas()

        # Property brand
        property_dict['agency'] = self.agency()
        property_dict['property_type'] = self.property_type
        property_dict['source_web'] = "inmoclick"
        property_dict['scrapped_date'] = datetime.today().strftime('%Y-%m-%d')

        return property_dict


class InmoclickSearchPage(object):
    soup = None
    property_type = None

    def __init__(self, soup, property_type: PropertyType):
        self.soup = soup
        self.property_type = property_type

    def max_page_number(self):
        last_page_url = urlparse(self.soup.find('span', attrs={'class': 'last'}).a.get('href'))
        last_page_number = int(last_page_url.query.split('&page=')[1])
        return last_page_number

    def search_items(self):
        articles = self.soup.find('div', attrs={'class': 'cont-articles'}).findAll('article')
        items = []
        for a in articles:
            items.append(SearchItem(a, self.property_type))
        return items

    def save(self, file_name):
        with open("soups/" + file_name + ".html", "w") as file:
            file.write(str(self.soup))