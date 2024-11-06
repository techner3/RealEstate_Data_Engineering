import os
import json
import time
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

class Scraping_CommonFloor:

    def __init__(self):

        self.website="https://www.commonfloor.com/listing-search?city=cities&cg=cities%20division&iscg=&search_intent=rent&polygon=1&page=1&page_size=30"
        self.headers= {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", 
        "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}
        self.base_url="https://www.commonfloor.com"
        self.city_list=["Chennai","Bangalore","Mumbai","Kolkata","Hyderabad","Kochi","Gurgaon","Madurai","Salem","Trichy"
        ,"Tirunelveli","Pune","Vishakapatnam"]
        self.state_list={"Chennai":"Tamil Nadu","Bangalore":"Karnataka","Mumbai":"Maharashtra","Kolkata":"West Bengal","Hyderabad":"Telangana",
            "Kochi":"Kerala","Gurgaon":"Haryana","Madurai":"Tamil Nadu","Salem":"Tamil Nadu","Trichy":"Tamil Nadu",
            "Tirunelveli":"Tamil Nadu","Pune":"Maharashtra","Visakhapatnam":"Andhra Pradesh"}
        
    def fetch_data(self,url):

        try:
            response = requests.get(url,headers=self.headers)
            if response.status_code==200:
                features_list=dict()
                content = response.content
                soup = BeautifulSoup(content,"html.parser")
                return soup
            else:
                return 
        except Exception as e:
            print(e)
        
    def extract_features(self,unordered_list):

        try:
            features = {}
            for li in unordered_list.find_all('li'):
                small = li.find('small')
                span = li.find('span')
                if small and span:
                    key = small.text.strip()
                    value = span.text.strip()
                    features[key] = value
            return features
        except Exception as e:
            print(e)

    def beautify_string(self,text:str)->str:

        try:
            words = text.title().split()
            lowercase_words = {"at", "in", "on", "and", "the", "of", "a"}
            for i, word in enumerate(words):
                if word.lower() in lowercase_words:
                    words[i] = word.lower()
            beautified_string = ' '.join(words)
            return beautified_string
        except Exception as e:
            print(e)

    def address_preprocess(self,address_text:str)->str:

        try:
            if address_text:
                address=self.beautify_string(" ".join(address_text.split("-in-")[-1].split("-")))
            else:
                address=""
            return address
        except Exception as e:
            print(e)
    
    def amenities_preprocess(self,amenities_list:list)->list:

        try:
            if amenities_list:
                amenities=[amenity.text.strip() for amenity in amenities_list]
            else:
                amenities=[]
            return amenities
        except Exception as e:
            print(e)

    def gather_data_url(self,url_list,location)->list[dict]:

        try:

            data_list=[]
            for property_url in url_list:

                url=self.base_url+property_url
                html_content=self.fetch_data(url)

                if html_content:

                    features_list=dict()
                    features_list.update(self.extract_features(html_content.find_all("ul",{"class":"_20"})[0]))
                    features_list.update(self.extract_features(html_content.find_all("ul",{"class":"_33"})[0]))
                    features_list["ID"]=property_url.split("/")[-1]
                    features_list["City"]=location
                    features_list["State"]=self.state_list[location]
                    features_list["Year"] = datetime.date.today().year
                    features_list["Rent"]=html_content.find_all("div",{"class":"subfeatures"})[0].text.strip()
                    features_list["Amenities"]=self.amenities_preprocess(html_content.find_all("div",{"id":"amenities"})[0].find_all('li'))
                    features_list["Address"]=self.address_preprocess(property_url.split("/")[-2])
                    data_list.append(features_list)
                
                else:
                    print(f"Failed to retieved data for {url}")

            return data_list
        
        except Exception as e:
            print(e)

    def gather_data(self)->list[dict]:

        try:

            total_results=[]
            for city in self.city_list:

                website=self.website.replace("cities",city)
                house_listing_urls=[]

                for i in range(10):

                    html_content=self.fetch_data(website)
                    if html_content:
                        house_listing=[element.find_all("a",{"class":""}) for element in html_content.find_all("div", {"class":"snb-content-list clearfix"})[0].find_all("div",{"class" : "snb-tile impressionAd"})]
                        house_listing_urls.extend([element[0]['href'] for element in house_listing if element!=[]])
                    else:
                        print(f"Failed to retieved data for {city}")
                        break
    
                total_results.extend(self.gather_data_url(list(set(house_listing_urls)),city))
                print(f"Scraping for {city} Completed")
                time.sleep(5)

            return total_results

        except Exception as e:
            print(f"Exception occured : {e}")

class Producer:

    def __init__(self,topic_name):

        self.security_protocol='SASL_SSL'
        self.sasl_mechanism='PLAIN'
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv('bootstrap_servers'),
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=os.getenv('sasl_plain_username'),
            sasl_plain_password=os.getenv('sasl_plain_password'),
            key_serializer=lambda v: json.dumps(v).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
    
    def return_producer(self):
        return self.producer

def scrap_stream(topic):

    try:
        scrap_obj=Scraping_CommonFloor()
        kafka_obj=Producer(topic)
        producer=kafka_obj.return_producer()
        
        df=pd.DataFrame(scrap_obj.gather_data())
        df.fillna("")

        for index, row in df.iterrows():

            value = row.to_dict()
            producer.send(topic=topic, value=value,key=str(index))
            print(f"{index} sent")
            time.sleep(1)
        print("Data Sent completed")
        producer.flush()

    except Exception as e:
        print(e)