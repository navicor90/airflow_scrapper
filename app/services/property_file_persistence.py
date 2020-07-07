from app.utils import _soup_filename
from app.services.file_service import CloudFileService
from app.models import PropertyType
from os import listdir
from datetime import datetime

cloud_dir_map = {PropertyType.LAND: "field",
                 PropertyType.HOUSE: "houses",
                 PropertyType.APARTMENT: "apartments"}


class PropertyFilePersistence(object):
    file_service = None

    def __init__(self, file_service: CloudFileService):
        self.file_service = file_service

    def save_search_pages(self, property_type: PropertyType):
        files = [_soup_filename(f) for f in listdir('soups')]
        for filename in files:
            self._save_search_file(filename, property_type)

    def save_search_page(self, page_number, property_type: PropertyType):
        filename = _soup_filename(page_number)
        self._save_search_file(filename, property_type)

    def _save_search_file(self, filename, property_type: PropertyType):
        today = datetime.today().strftime('%Y-%m-%d')
        cloud_path = f"search_page_htmls/{cloud_dir_map[property_type]}/{today}"
        self.file_service.save(filename, cloud_path)
