from bs4 import BeautifulSoup
import sys
import logging
logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)

logger = logging.getLogger("airflow.task")


class B4SApartmentExtractor:
    """
    Abstracts BeatifulSoup's way to find html objects,
    with the only objective to find the objects for the GetApartmentsInfo class.
    """

    def __init__(self,rent_info: str) -> None:
        self.rent_info=rent_info

    def find_apartment_rent_price(self) -> int:
        """
        Return Apartment's rent price, through the HTML's element.
        """
        try:
            logger.debug("Getting Apartment prices information.")
            price_element = self.rent_info.find('div', class_='listing-price')
            price = price_element.find('p', class_='l-text--variant-body-small').text
            return price
        except:
            price=""
            return price
    
    def find_apartment_total_price(self) -> int:
        """
        Return Apartment's total price, through the HTML's element.
        """
        try:
            logger.debug("Getting Apartment total price information.")
            total_price_element = self.rent_info.find('div', class_='listing-price')
            total = total_price_element.find('p', class_='l-text--variant-heading-small').text
            return total
        except:
            total=""
            return total
    
    def find_apartment_address(self) -> str:
        """
        Return Apartment's address, through the HTML's element.
        """
        try:
            logger.debug("Getting Apartment address information.")
            address_element=self.rent_info.find('section',class_="card__location")
            address = address_element.find('h2', class_='card__address').text
            street = address_element.find('p', class_='card__street').text
            complete_address=f"{street}, {address}"
            return complete_address
        except:
            complete_address=""
            return complete_address



    def find_apartment_floor_size(self) -> str:
        """
        Return Apartment's Floor Size, through the HTML's element.
        """
        try:
            logger.debug("Getting Apartment floor_size information.")
            floor_size=self.rent_info.find('p', itemprop='floorSize').text.strip()
            return floor_size
        except:
            floor_size=""
            return floor_size

    # def find_apartment_iptu(self) -> int:
    #     """
    #     Return Apartment's IPTU, through the HTML's element.
    #     """
    #     logger.debug("Getting Apartment iptu information.")
    #     iptu_element = self.rent_info.find('li', class_="card-price__item iptu text-regular")
    #     iptu=iptu_element.find('span',class_="card-price__value").get_text(strip=True) if iptu_element else 0
    #     return iptu
    
    def find_apartment_number_of_rooms(self) -> int:
        """
        Return Apartment's Number of Rooms, through the HTML's element.
        """
        try:
            logger.debug("Getting Apartment number_of_rooms information.")
            number_of_rooms=self.rent_info.find('p', itemprop='numberOfRooms').text.strip()
            return number_of_rooms
        except:
            number_of_rooms=""
            return number_of_rooms
    
    def find_apartment_number_of_bathrooms(self) -> int:
        """
        Return Apartment's Number of Bathrooms, through the HTML's element.
        """
        try:
            logger.debug("Getting Apartment number_of_bathrooms information.")
            number_of_bathrooms = self.rent_info.find('p', itemprop='numberOfBathroomsTotal').text.strip()            
            return number_of_bathrooms
        except:
            number_of_bathrooms=""
            return number_of_bathrooms
    # def find_apartment_parking_spots(self) -> int:
    #     """
    #     Return Apartment's Number of Parking Spots, through the HTML's element.
    #     """
    #     logger.debug("Getting Apartment parking information.")
    #     parking_element = self.rent_info.find('li', class_="feature__item text-small js-parking-spaces")
    #     parking=parking_element.find_all('span')[1].get_text(strip=True) if parking_element else 0        
    #     return parking
    
    # def find_apartment_description(self) -> str:
    #     """
    #     Return Apartment's Renting Description, through the HTML's element.
    #     """
    #     logger.debug("Getting Apartment description information.")
    #     description_element=self.rent_info.find('span', class_="simple-card__text text-regular")
    #     description=description_element.next.strip() if description_element else str()
    #     return description

