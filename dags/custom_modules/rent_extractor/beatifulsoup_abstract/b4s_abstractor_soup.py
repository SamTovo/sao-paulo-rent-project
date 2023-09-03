from bs4 import BeautifulSoup 

class B4SApartmentExtractorSoup:

    def __init__(self,apartment_html:str) -> None:
        self.apartment_html = apartment_html

    def get_apartment_html_soup(self):
        self.soup = BeautifulSoup(self.apartment_html,'html.parser')

    def get_general_apartment_info(self) -> str:
        total_rent_infos=self.soup.find_all( class_="l-card__content")
        return total_rent_infos