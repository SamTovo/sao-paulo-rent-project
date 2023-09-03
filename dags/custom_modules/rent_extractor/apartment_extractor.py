import requests as r
import re
import pandas as pd
import sys
import time
# from custom_modules.rent_extractor.beatifulsoup_abstract.b4s_abstractor import B4SApartmentExtractor
# from custom_modules.rent_extractor.beatifulsoup_abstract.b4s_abstractor_soup import B4SApartmentExtractorSoup
from custom_modules.rent_extractor.beatifulsoup_abstract.b4s_abstractor import B4SApartmentExtractor
from custom_modules.rent_extractor.beatifulsoup_abstract.b4s_abstractor_soup import B4SApartmentExtractorSoup
import logging

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)

logger = logging.getLogger(__name__)




class GetApartmentsInfo:

    def __init__(self,first_url_page:int ,last_url_page:int):
        self.page_number=1
        self.price=[]
        self.address=[]
        # self.condominium=[]
        self.floor_size=[]
        # self.iptu=[]
        self.number_of_rooms=[]
        self.number_of_bathrooms=[]
        # self.parking_spots=[]
        # self.description=[]
        self.first_url_page=first_url_page
        self.last_url_page=last_url_page
        self.total_price=[]
        

    def _get_apartment_page(self):
        logger.info(f"Making request number {self.page_number}")
        self.url = f'https://www.zapimoveis.com.br/aluguel/apartamentos/sp+sao-paulo/?pagina={self.page_number}&tipos=apartamento_residencial,studio_residencial,kitnet_residencial,casa_residencial,,condominio_residencial,casa-vila_residencial,cobertura_residencial,flat_residencial,loft_residencial,lote-terreno_residencial,granja_residencial&transacao=aluguel&onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,,city,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.555771,-46.639557,'
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
        self.apartments_page=r.get(self.url,headers=self.headers)
        

    def _generate_html_page(self):
        logger.info(f"Getting the HTML page")
        self.apartment_html=self.apartments_page.text
        self.b4s_object=B4SApartmentExtractorSoup(self.apartment_html)
        self.soup = self.b4s_object.get_apartment_html_soup()

    def _extract_apartment_info_columns(self):
        """
        Extracts and returns the websites apartments and its infos in columns for each of them.
        """
        logger.info(f"Initializing extraction of apartment renting pages")
        for page in range(self.first_url_page,self.last_url_page):
            self.page_number=page
            self._get_apartment_page()
            self._generate_html_page()
            total_rent_infos=self.b4s_object.get_general_apartment_info()
            logger.debug(len(total_rent_infos))
            for rent_info in total_rent_infos:
                b4s_apartment_infos=B4SApartmentExtractor(rent_info)

                self.price.append(b4s_apartment_infos.find_apartment_rent_price())
                self.address.append(b4s_apartment_infos.find_apartment_address())
                # self.condominium.append(b4s_apartment_infos.find_apartment_codominium())
                self.floor_size.append(b4s_apartment_infos.find_apartment_floor_size())
                # self.iptu.append(b4s_apartment_infos.find_apartment_iptu())
                self.number_of_rooms.append(b4s_apartment_infos.find_apartment_number_of_rooms())
                self.number_of_bathrooms.append(b4s_apartment_infos.find_apartment_number_of_bathrooms())
                self.total_price.append(b4s_apartment_infos.find_apartment_total_price())
                # self.parking_spots.append(b4s_apartment_infos.find_apartment_parking_spots())
                # self.description.append(b4s_apartment_infos.find_apartment_description())
                time.sleep(5)
    def _check_if_result_is_none(self,df):
        if df is None:
            raise ValueError("The DataFrame is Empty")
        else:
            pass


    def generate_pandas_apartment_info(self) -> pd.DataFrame:
        """
        Generate, from the _extract_apartment_info_columns function, a pandas Dataframe with each list as a column.
        """
        

        self._extract_apartment_info_columns()
        apartment_infos_dict={
            "price":self.price,
            "total_price":self.total_price,
            "address":self.address,
            # "condominium":self.condominium,
            "floor_size":self.floor_size,
            # "iptu":self.iptu,
            "number_of_rooms":self.number_of_rooms,
            "number_of_bathrooms":self.number_of_bathrooms,
            # "parking_spots":self.parking_spots,
            # "description":self.description
        }
        logger.debug(apartment_infos_dict)
        logger.info(f"Initializing transformation to Pandas")
        apartment_infos_df=pd.DataFrame(apartment_infos_dict)
        self._check_if_result_is_none(apartment_infos_df)
        return apartment_infos_df
    

def apartment_extractor():
    ap=GetApartmentsInfo(1,10)
    print(ap.generate_pandas_apartment_info())


if __name__=="__main__":
    apartment_extractor()        






