# -*- coding: utf-8 -*-
from airflow.utils.decorators import apply_defaults
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
from enum import Enum
from selenium_plugin.operators.selenium_operator import SeleniumOperator
import logging
import os

if not os.path.exists('soups'):
    os.makedirs('soups')

class SgtTurnStatusWebOperator(SeleniumOperator):
    '''
    Turn Taker Web Operator
    '''
    template_fields = ['script_args']
    turns_soup_file = "soups/turns.html"

    class TurnOffices(Enum):
        BARCELONA = 'Barcelona'

    class TurnTypes(Enum):
        SWITCH_LICENSE = u'Canjes de permisos de conducción'

    class TurnCountries(Enum):
        ARGENTINA = 'Argentina'

    @apply_defaults
    def __init__(self,
                 office: TurnOffices,
                 ttype: TurnTypes,
                 country: TurnCountries,
                 *args,
                 **kwargs):
        def get_soups_from_turn_taker(selenium_driver):
            turn_taker = CarLicenseTurnTaker(selenium_driver)
            turn_taker.save_soup_for_available_turns(office=office.value,
                                                     ttype=ttype.value,
                                                     country=country.value,
                                                     filename=self.turns_soup_file)
        super().__init__(script=get_soups_from_turn_taker, script_args=[], *args, **kwargs)

    def _read_turns_soup(self):
        with open(self.turns_soup_file, "r") as f:
            contents = f.read()
            soup = BeautifulSoup(contents, 'lxml')
            return soup

    def execute(self, context):
        super().execute(context)
        soup = self._read_turns_soup()
        web_status = WebTurnStatus(soup)
        r = web_status.has_available_turns()
        logging.log("------------- TERMINADO ----------")
        return r


class CarLicenseTurnTaker(object):
    url = 'https://sedeapl.dgt.gob.es:7443/WEB_NCIT_CONSULTA/solicitarCita.faces'

    def __init__(self, driver):
        self.driver = driver

    def _load_turns_web(self):
        self.driver.get(self.url)

    def _set_office_in_form(self, office):
        selector_html_name = "publicacionesForm:oficina"
        html_opt_group = office[0].upper()
        xpath = f"//select[@name='{selector_html_name}']/optgroup[@label='{html_opt_group}']/option[contains(text(),'{office}')]"
        self.driver.find_element_by_xpath(xpath).click()

    def _set_turn_type_in_form(self, ttype):
        selector_html_name = "publicacionesForm:tipoTramite"
        xpath = f"//select[@name='{selector_html_name}']/option[contains(text(),'{ttype}')]"
        self.driver.find_element_by_xpath(xpath).click()

    def _set_license_country_in_form(self, country):
        selector_html_name = "publicacionesForm:pais"
        xpath = f"//select[@name='{selector_html_name}']/option[contains(text(),'{country}')]"
        self.driver.find_element_by_xpath(xpath).click()

    def _hover_element(self, html_element):
        actions = ActionChains(self.driver)
        actions.move_to_element(html_element)
        actions.perform()

    def _submit_form(self):
        button = self.driver.find_element_by_xpath("//input[@name='publicacionesForm:j_id70']")
        self._hover_element(button)
        button.click()

    def _save_soup_now(self, filename):
        content = self.driver.page_source
        soup = BeautifulSoup(content, features="lxml")
        with open(filename, "w+") as file:
            file.write(str(soup))

    def save_soup_for_available_turns(self, office: str, ttype: str, country: str, filename: str):
        self._load_turns_web()
        self._set_office_in_form(office)
        self._set_turn_type_in_form(ttype)
        self._set_license_country_in_form(country)
        self._submit_form()
        self._save_soup_now(filename)


class TurnResponseStatus(Enum):
    SERVER_PROBLEMS = "SERVER_PROBLEMS"
    NOT_AVAILABLE_TURNS = "NOT_AVAILABLE_TURNS"
    AVAILABLE_TURNS = "AVAILABLE TURNS"


class WebTurnStatus(object):
    there_are_not_turns_message = "El horario de atención al cliente está completo para los próximos días. Inténtelo " \
                                  "más tarde. "
    server_is_collapsed_message = "Estamos recibiendo un número muy elevado de accesos que no nos permiten procesar " \
                                  "tu petición. Por favor, inténtalo de nuevo pasados unos minutos. "

    def __init__(self, soup):
        self.soup = soup

    def _get_status_message(self):
        return self.soup.find('div', attrs={'class': 'buscadorInterno'}) \
                        .find("div", attrs={'id': 'publicacionesForm:j_id72'}).text

    def get_turn_status(self):
        message = self._get_status_message()
        if self.there_are_not_turns_message in message:
            return TurnResponseStatus.NOT_AVAILABLE_TURNS
        elif self.server_is_collapsed_message in message:
            return TurnResponseStatus.SERVER_PROBLEMS
        else:
            return TurnResponseStatus.AVAILABLE_TURNS

    def has_available_turns(self):
        turn_status = self.get_turn_status()
        if turn_status == TurnResponseStatus.AVAILABLE_TURNS:
            return True
        elif turn_status == TurnResponseStatus.SERVER_PROBLEMS:
            raise Exception("server SGT has problems")
        return False