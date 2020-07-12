from app.services.file_service import CloudFileService
from app.models import PropertyType
import os
from datetime import datetime

cloud_dir_map = {PropertyType.LAND: "field",
                 PropertyType.HOUSE: "houses",
                 PropertyType.APARTMENT: "apartments"}


class PropertyFilePersistence(object):
    file_service = None

    def __init__(self, file_service: CloudFileService):
        self.file_service = file_service

    def cloud_save_search_pages(self, property_type: PropertyType):
        files = PropertyFilePersistence.local_soups_files(property_type)
        for filename in files:
            self._save_search_file(filename, property_type)

    def cloud_save_search_page(self, page_number, property_type: PropertyType):
        filename = PropertyFilePersistence.local_full_filename(property_type, page_number)
        self._save_search_file(filename, property_type)

    def _save_search_file(self, filename, property_type: PropertyType):
        today = datetime.today().strftime('%Y-%m-%d')
        cloud_path = f"search_page_htmls/{cloud_dir_map[property_type]}/{today}"
        self.file_service.save(filename, cloud_path)

    @classmethod
    def local_save(cls, property_type, page_number, soup):
        ffilename = cls.local_full_filename(property_type, page_number)
        os.makedirs(os.path.dirname(ffilename), exist_ok=True)
        with open(ffilename, "w") as file:
            file.write(str(soup))

    @classmethod
    def local_path(cls, property_type):
        return "soup/"+property_type.value

    @classmethod
    def local_soups_files(cls, property_type):
        soups_dir = cls.local_path(property_type)
        files = [os.path.join(soups_dir, f) for f in os.listdir(soups_dir)]
        return files

    @classmethod
    def local_full_filename(cls, property_type, page_number):
        path = cls.local_path(property_type)
        return path + "/" + str(page_number) + ".html"