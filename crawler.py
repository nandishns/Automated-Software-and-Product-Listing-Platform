import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from kafka import KafkaProducer
import uuid
import json
import requests
from bs4 import BeautifulSoup
import threading
from threading import Thread
from collections import Counter
from urllib.parse import urljoin

def setup_kafka_producer():
    # Initialize Kafka producer with bootstrap servers
    return KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def scrape_producthunt(producer):

    def scrape_base_page(url):
        try:
            # Send a GET request to the webpage
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the HTML content of the page with BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            print(f"Failed to retrieve data from {url}: {e}")
            return []

        # List to store the collected hrefs
        collected_hrefs = []

        # Find all div elements with the specific class
        divs = soup.find_all('div', class_='styles_item__Dk_nz my-2 flex flex-1 flex-row gap-2 py-2 sm:gap-4')
        for div in divs:
            # Find all anchor tags within each div
            anchor_tags = div.find_all('a')
            for a in anchor_tags:
                href = a.get('href')  # Get the href attribute
                if href and '/posts/' in href:
                    full_url = urljoin(url, href)
                    collected_hrefs.append(full_url)

        return collected_hrefs

    def extract_additional_data(post_urls):
        base_url = 'https://www.producthunt.com'
        posts_data = []
        counter = Counter(post_urls)

        # Create a new list, removing one occurrence of each duplicate
        new_post_urls = []
        for url in post_urls:
            if counter[url] > 1:
                counter[url] -= 1
            else:
                new_post_urls.append(url)

        post_urls = new_post_urls

        for url in post_urls:
            try:
                post_response = requests.get(url)
                post_response.raise_for_status()
                
                # Parse the HTML content of the post page
                post_soup = BeautifulSoup(post_response.text, 'html.parser')
                
                # Extract the h1 text
                post_h1 = post_soup.find('h1').text if post_soup.find('h1') else "No H1 tag found"
                
                # Extract the text from divs with the specific class
                target_divs = post_soup.find_all('div', class_='styles_htmlText__eYPgj text-16 font-normal text-dark-grey')
                div_text = ' '.join(div.text for div in target_divs)
                
                # Store the extracted data in the dictionary
                data = {'url': url, 'name': post_h1, 'description': div_text}
                
                # Send the data to Kafka
                #producer.send('Software', value=data)
                print(post_h1)
                print(url)
                print(div_text)
            except requests.RequestException as e:
                print(f"Failed to retrieve or process data from {url}: {e}")
                continue  # Skip this URL and continue with the next one

            posts_data.append(data)

        return posts_data

    # URL of the webpage you want to scrape
    base_url = 'https://www.producthunt.com/all'

    # First, scrape the base page to get all relevant hrefs
    post_hrefs = scrape_base_page(base_url)

    # Then, extract additional data using the collected hrefs
    posts_info = extract_additional_data(post_hrefs)

    # Output the extracted data
    for post in posts_info:
        print(f"URL: {post['url']}")
        print(f"Name: {post['name']}")
        print(f"Description: {post['description']}")
        print("-" * 80)  # Print a separator line



def crawl_betalist(startups_and_links_file,producer):

    def extract_urls_from_json(filename, start_idx, end_idx):
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Determine the elements to read
        data_chunk = data[start_idx:end_idx]
        
        # Iterate through the data chunk and extract URLs
        urls = []
        for entry in data_chunk:
            if "link_topic" in entry:
                urls.append(entry["link_topic"])
        
        return urls

    def scrape_category_details(url):
        response = requests.get(url)
        details = []
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            container = soup.find('div', class_='infinite-startups')
            if container:
                links = container.find_all('a', class_="block whitespace-nowrap text-ellipsis overflow-hidden font-medium")
                for link in links:
                    # For each link, store both its text and the href attribute
                    details.append({
                        "text": link.get_text(strip=True),
                        "href": link.get('href')  # Extract the href attribute
                    })
        else:
            print(f"Failed to fetch the page for {url}, status code: {response.status_code}")
        return details

    def scrape_details_from_urls(urls):
        startup_details = []

        for url in urls:
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                title = soup.find('h2').get_text(strip=True) if soup.find('h2') else 'Title Not Found'
                main_content_div = soup.find('div', class_='main content')
                description = ' '.join(p.get_text(strip=True) for p in main_content_div.find_all('p')) if main_content_div else 'Description Not Found'
                startup_details.append({"title": title, "description": description})
            else:
                print(f"Failed to fetch the page for URL: {url}, status code: {response.status_code}")
                startup_details.append({"title": 'Title Not Found', "description": 'Description Not Found'})
        return startup_details

    def check_for_new_products(url, filename=startups_and_links_file):
        category_details = scrape_category_details(url)
        
        # Load existing data from the JSON file
        with open(filename, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
        
        existing_hrefs = set()
        for entry in existing_data:
            if entry["link_topic"] == url and entry["startups"]:
                for startup in entry["startups"]:
                    existing_hrefs.add(startup["href"])

        new_products_found = False
        
        # Check if any new products are found
        new_product_hrefs = []
        for detail in category_details:
            if detail["href"] not in existing_hrefs:
                new_products_found = True
                new_product_hrefs.append(detail)

        # Print new product names and scrape details
        if new_product_hrefs:
            
            for detail in new_product_hrefs:
                print(f"New product found: {detail['text']}")
                base_url = "https://betalist.com"
                href = detail['href']
                full_url = f"{base_url}{href}"
                startup_details = scrape_details_from_urls([full_url])
                for startup in startup_details:
                    #print(f"text: {detail['text']}")
                    #print(f"Title: {startup['title']}")
                    #print(f"Description: {startup['description']}")
                    data = {
                        "text": detail['text'],
                        "title": startup['title'],
                        "description": startup['description']
                    }
                    producer.send('Software', value=data)
                    print("Data sent to Kafka topic 'Software'") #### send details of new product through kafka
        
        # Append new products to the JSON file
        if new_product_hrefs:
            for detail in new_product_hrefs:
                entry = {
                    "text": detail["text"],
                    "href": detail["href"]
                }
                for existing_entry in existing_data:
                    if existing_entry["link_topic"] == url:
                        existing_entry["startups"].append(entry)
                        break
                else:
                    existing_data.append({"link_topic": url, "startups": [entry]})
            
            # Write the updated data back to the JSON file
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=4)

        return new_products_found

    def run_crawler(start_idx, end_idx,producer):
        filename = startups_and_links_file
        urls = extract_urls_from_json(filename, start_idx, end_idx)
        for url in urls:
            new_products = check_for_new_products(url,producer)

            if not new_products:
                print("No new products found.")

    # JSON filename
    filename = startups_and_links_file

    # Calculate the number of elements
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    num_elements = len(data)

    # Divide the elements into two parts
    mid_idx = num_elements // 2

    # Create two threads to run the crawlers simultaneously
    thread1 = threading.Thread(target=run_crawler, args=(0, mid_idx,producer))
    thread2 = threading.Thread(target=run_crawler, args=(mid_idx, num_elements,producer))

    # Start the threads
    thread1.start()
    thread2.start()

    # Wait for the threads to finish
    thread1.join()
    thread2.join()




def sideproject_crawler(producer,sideprojects_info_file):

    def setup_chromedriver():
        options = webdriver.ChromeOptions()
        #options.add_argument('--headless')  # Run Chrome in headless mode for efficiency
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        return driver

    def scrape_data(base_url, max_offset, max_date, known_products,producer):
        driver = setup_chromedriver()
        new_products = []
        date_format = "%d %b, %Y"  # Adjust this format to match your date strings

        for offset in range(0, max_offset, 20):
            url = f"{base_url}{offset}"
            print(f"Scraping {url}")
            driver.get(url)
            
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'body')))
            
            product_names = WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'mt-5')))
            descriptions = WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'description')))
            date_selector = 'div.mt-6 > span:last-child'
            dates = WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, date_selector)))
            
            for i in range(min(len(product_names), len(descriptions), len(dates))):
                product_date = datetime.strptime(dates[i].text.strip(), date_format)
                if product_date < max_date:
                    print("Stopping scrape as product dates are older than the most recent known date.")
                    driver.quit()  # Ensure the driver quits if we break the loop
                    return new_products

                product_info = {
                    "_id": str(uuid.uuid4()).replace('-', ''),
                    "name": product_names[i].text.strip(), 
                    "description": descriptions[i].text.strip(),
                    "date": product_date  # Store as datetime object for reliable sorting and comparison
                }

                if product_info['name'] not in known_products or product_date > max_date:
                    new_products.append(product_info)
                    # Send data to Kafka
                    producer.send('Software', product_info)
                    producer.flush()  # Ensure all messages are sent
                    print(f"Sent new product to Kafka: {product_info['name']}")


        driver.quit()
        return new_products

    def read_csv(file_path):
        try:
            df = pd.read_csv(file_path, parse_dates=['date'])
        except pd.errors.EmptyDataError:
            df = pd.DataFrame(columns=['_id', 'name', 'description', 'date'])  # Setup with correct columns if CSV is empty
        max_date = df['date'].max() if not df.empty else datetime.min
        known_products = df['name'].tolist()
        return df, max_date, known_products

    def update_csv(df, new_products, file_path):
        new_df = pd.DataFrame(new_products)
        if not new_df.empty:
            updated_df = pd.concat([df, new_df], ignore_index=True)
            updated_df.sort_values(by='date', ascending=False, inplace=True)  # Sort before formatting dates to strings
            updated_df['date'] = updated_df['date'].apply(lambda x: x.strftime('%d %b, %Y').lstrip('0'))  # Convert dates to strings, remove leading zero
            updated_df.to_csv(file_path, index=False)
            print("CSV file has been updated with new products.")
        else:
            print("No new products to add.")

# Main execution
    csv_file_path = sideprojects_info_file  # Replace with your actual CSV file path
    df, max_date, known_products = read_csv(csv_file_path)

    base_url = "https://www.sideprojectors.com/#/ZmM4rtgJE8/sell,cofounder,show,sold,me/saas,shop,blog,website,mobile,desktop,browser,domain,other/all/1/all/all/1/all/all/all/all/all/created_at/desc/20/"
    max_offset = 200  # Adjust as needed

    new_products = scrape_data(base_url, max_offset, max_date, known_products,producer)
    update_csv(df, new_products, csv_file_path)




def main():
    producer = setup_kafka_producer()

    thread1 = Thread(target=sideproject_crawler, args=(producer,'Products.product_info3.csv',)) 
    thread2 = Thread(target=crawl_betalist, args=('output_sites2.json',producer,))   
    thread3 = Thread(target=scrape_producthunt, args=(producer,))  
    

    thread1.start()
    thread2.start()
    thread3.start()

    # Wait for both threads to complete
    thread1.join()
    thread2.join()
    thread3.join()
    
    producer.close()

if __name__ == "__main__":
    main()
