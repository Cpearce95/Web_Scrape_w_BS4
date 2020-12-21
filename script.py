##Imports
import pandas as pd 
import numpy as np 
import sqlalchemy as sa 
from sqlalchemy import create_engine
import os
from bs4 import BeautifulSoup
import requests


##Defining Base Url for each section to scrape 
kitchen_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=kitchen-laundry&cp=' #26 pages
home_garden_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=home-furniture&cp=' #42 pages
technology_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=home-entertainment&cp=' #8 pages
health_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=health-beauty&cp=' # 6 pages
computers_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=office-computers&cp=' # 3 pages
kids_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=kids-nursery&cp=' #11 pages
cars_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=outdoors-diy&cp=' # 12 pages
mens_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=mens&cp=' # 9 pages
womens_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=womens&cp=' # 34 pages 
holdays_url = 'https://www.offeroftheday.co.uk/travel' # 1 page
christmas_url = 'https://www.offeroftheday.co.uk/category.asp?pp=93&order=pop&cat=christmas-gifts&cp='#3 pages
urls_list = [] ##Empty list to hold final list of urls to iterate through
urls = [home_garden_url, kitchen_url,technology_url, health_url, computers_url ,kids_url ,cars_url ,mens_url ,womens_url, holdays_url, christmas_url] ##List of base urls to loop through
c = 0 #Initialise counter var as 0 

##Connecting to Database using SQLAlchemy
server = 'server01' ##Server to connect to 
db = 'hotel_bookings_db' ##Database to connect to
engine = sa.create_engine(f'mssql+pyodbc://{server}/{db}?Trusted_Connection=yes&Driver=ODBC Driver 17 for SQL Server') ##SQL conn string with params
con = engine.connect() ##Initialise connection to sa engine
print ("Successfully connected to db") ##output connected to DB if successful

##Building url list from base urls 
##For loop to build the full url list to scrape stemming from the base urls defined at start of program 
for url in urls:
    if url == kitchen_url:
        c = 0
        for i in range(26):
            c += 1
            print(kitchen_url + str(c))
            new_url = kitchen_url + str(c)
            urls_list.append(new_url)
    elif url == home_garden_url:
        c = 0
        for i in range(42):
            c += 1
            print(home_garden_url + str(c))
            new_url = home_garden_url + str(c)
            urls_list.append(new_url)
    elif url == technology_url:
        c = 0
        for i in range(8):
            c += 1
            print(technology_url + str(c))
            new_url = technology_url + str(c)
            urls_list.append(new_url)
    elif url == health_url:
        c = 0
        for i in range(6):
            c += 1
            print(health_url + str(c))
            new_url = health_url + str(c)
            urls_list.append(new_url)
    elif url == computers_url:
        c = 0
        for i in range(3):
            c += 1
            print(computers_url + str(c))
            new_url = computers_url + str(c)
            urls_list.append(new_url)
    elif url == kids_url:
        c = 0
        for i in range(11):
            c += 1
            print(kids_url + str(c))    
            new_url = kids_url + str(c)
            urls_list.append(new_url)
    elif url == cars_url:
        c = 0
        for i in range(12):
            c += 1
            print(cars_url + str(c))  
            new_url = cars_url + str(c)
            urls_list.append(new_url)
    elif url == mens_url:
        c = 0
        for i in range(9): 
            c += 1
            print(mens_url + str(c))
            new_url = mens_url + str(c)
            urls_list.append(new_url)
    elif url == womens_url:
        c = 0
        for i in range(34): 
            c += 1
            print(womens_url + str(c))
            new_url = womens_url + str(c)
            urls_list.append(new_url)
    elif url == holdays_url:
        c = 0
        for i in range(1): 
            c += 1
            print(holdays_url + str(c))
            new_url = holdays_url + str(c)
            urls_list.append(new_url)
    if url == christmas_url:
        c = 0
        for i in range(3): 
            c += 1
            print(christmas_url + str(c))
            new_url = christmas_url + str(c)
            urls_list.append(new_url)
            
##Scrape section

##Empty list for each column to hold data
Item_Name,Price,Discount,Hrefs,Shops,Sections,Image_List = [],[],[],[],[],[],[] 

##Looping through URLs and scrape required information
for url in urls_list: 
    html = requests.get(url)
    soup = BeautifulSoup(html.text, 'html.parser')
    section = soup.find('h1').text
    for name in soup.find_all('div', class_ = 'tile'):
        try:
            itemname = name.find('a', class_ = 'tt').text
            price = name.find('div', class_ = 'nw').text
            discount = name.find('div', class_ = 'sv').text
            href = name.find('a', class_ = 'tt').get('href')
            image = name.find('img').get('data-src')
            href = 'https://www.offeroftheday.co.uk' + href
            for li in name.find_all('li'):
                if li.has_attr('data-shop'):
                    s = li.get('data-shop')
                    Shops.append(s)
            Item_Name.append(itemname)
            Price.append(price)
            Discount.append(discount)
            Hrefs.append(href)
            Sections.append(section[18:])
            Image_List.append(image)
        except:
            print ("Uh oh")
            
##Creating a dict from scraped lists   
dict = {'Section': Sections,'Item': Item_Name , 'Price': Price , 'Discount': Discount,'Url': Hrefs , 'Shop': Shops, 'Image': Image_List}  

##Creating a dataframe from the dict above
df = pd.DataFrame.from_dict(dict) 

##Cleaning the data to achieve conversion to desired data types and derive original prices of items as new column
df['Price'] = df['Price'].str.replace('£', '') 
df['Price'] = df['Price'].str.replace(',', '')
df['Price'] = df['Price'].str.replace('*', '')
df['Discount'] = df['Discount'].str.replace('% OFF', '')
df['Price'] = df['Price'].astype(float)
df['Discount'] = df['Discount'].astype(int)
df['Shop'] = df['Shop'].str.title()
df['Original_Price'] = 100-df['Discount']
fraction = df['Original_Price']/100
df['Original_Price'] = df['Price'] / fraction

##Writing dataframe to sql database using connection string defined in program
df.to_sql('tbl_discountproducts_v1',con=con, if_exists='replace', index=False)