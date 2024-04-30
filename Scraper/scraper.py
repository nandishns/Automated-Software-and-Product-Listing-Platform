from queue import Queue
import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, TimeoutException
from selenium.webdriver.common.keys import Keys
import os, time, json, requests
from kafka import KafkaProducer
from threading import Thread
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from collections import Counter
from dotenv import load_dotenv
from selenium.webdriver.chrome.service import Service

load_dotenv()
TWITTER_USER_NAME=os.getenv("TWITTER_USER_NAME")
TWITTER_PASSWORD=os.getenv("TWITTER_PASSWORD")



def setup_webdriver():

    # ChromeDriverManager().clear_cache()
    chrome_options = Options()   
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.binary_location = "/usr/bin/google-chrome" 

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)
 

def setup_kafka_producer():
    print("Setting up Kafka producer")
    return KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def scrape_slashdot(producer):
    print("Scraping Slashdot")
    try:
        driver = setup_webdriver()
        # driver.maximize_window()
        wait = WebDriverWait(driver, 10)
        driver.get("https://slashdot.org/")
        # time.sleep(5)
        all_element = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Software")))
        all_element.click()

        while True:
            products = driver.find_elements(By.CSS_SELECTOR, "div.result-heading-texts")
            for product in products:
                product_name = product.find_element(By.CSS_SELECTOR, "h3").text
                description = product.find_element(By.CSS_SELECTOR, "div.description").text
                data = {"name": product_name, "description": description}
                # Send data to Kafka
                producer.send('Software', value=data)

            time.sleep(5)
            try:
                next_button = driver.find_element(By.LINK_TEXT, "Next")
                next_button.click()
            except:
                print("No more pages to scrape for Slashdot.")
                break
            # finally:
            #   driver.quit()
    except NoSuchElementException as e:
       
        # print(f"Element not found: {e}")
        pass
    except ElementNotInteractableException as e:
     
        # print(f"Element not interactable: {e}")
        pass
    except TimeoutException as e:
       
        # print(f"Operation timed out: {e}")
        pass
    except Exception as e:
        
        # print(f"Error in scrape_slashdot: {e}")
        pass


def scrape_producthunt(producer):
    print("Scraping Product Hunt")
    def scrape_base_page(url):
        try:
            
            response = requests.get(url)
            response.raise_for_status() 

            # Parse the HTML content of the page with BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            # print(f"Failed to retrieve data from {url}: {e}")
            return []
        collected_hrefs = []

       
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
                
                
                post_soup = BeautifulSoup(post_response.text, 'html.parser')
                
              
                post_h1 = post_soup.find('h1').text if post_soup.find('h1') else "No H1 tag found"
                
                target_divs = post_soup.find_all('div', class_='styles_htmlText__eYPgj text-16 font-normal text-dark-grey')
                div_text = ' '.join(div.text for div in target_divs)
                
            
                data = {'url': url, 'name': post_h1, 'description': div_text}
                
                # Send the data to Kafka
                producer.send('Software', value=data)
            except requests.RequestException as e:
                # print(f"Failed to retrieve or process data from {url}: {e}")
                continue  

            posts_data.append(data)

        return posts_data

    
    base_url = 'https://www.producthunt.com/all'

   
    post_hrefs = scrape_base_page(base_url)

   
    posts_info = extract_additional_data(post_hrefs)
       

def scrape_sideprojectors(producer):
    print("Scraping SideProjectors")
    try:
        driver = setup_webdriver()
        driver.maximize_window()
        wait = WebDriverWait(driver, 10)
        driver.get("https://www.sideprojectors.com/#/")
     
        ids = ["input-project-type-blog", "input-project-type-domain", "input-project-type-other"]

        for id_value in ids:
            try:
              
                element = driver.find_element(By.ID, id_value)
                element.click()
            except:
                continue

        search_button=driver.find_element(By.XPATH, '//button[text()="Search"]')
        search_button.click()

        time.sleep(3)

        while True:
            last_height = driver.execute_script("return document.body.scrollHeight")
            products = driver.find_elements(By.CSS_SELECTOR, "a.project-item")
            for product in products:
                product_name = product.find_element(By.CSS_SELECTOR, ".name").text
                description = product.find_element(By.CSS_SELECTOR, "div.description").text
                date_span = product.find_elements(By.CSS_SELECTOR, "div.mt-6.flex.items-center span.gray-text")[-1].text
                product_doc = {"name": product_name, "description": description,"date": date_span}
                producer.send('Software', value=product_doc)

            
            time.sleep(5)

            try:
                next_button = driver.find_element(By.XPATH, '//button[text()="Next"]')
                next_button.click()
            except:
                print("No more pages to scrape for SideProjectors.")
                break
        # driver.quit()

    except Exception as e:
        pass
        # print(f"Error in scrape_slashdot: {e}")


def scrape_twitter(producer):
    print("Scraping Twitter")
    """Function to log into Twitter and scrape tweets based on a hashtag."""
    driver = setup_webdriver()
    try:
        # Navigating to Twitter's login page
        driver.get("https://twitter.com/login")
        time.sleep(3)  # Wait to ensure the page is fully loaded

        # Log in process
        username_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "text")))
        username_input.send_keys(TWITTER_USER_NAME)
        # print(TWITTER_USER_NAME)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Next']"))).click()
        
        password_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "password")))
        password_input.send_keys(TWITTER_PASSWORD)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Log in']"))).click()

        # Wait for home page to load by checking presence of Home timeline
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid="primaryColumn"]')))

        # Perform search
        search_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='SearchBox_Search_Input']")))
        search_input.send_keys("#b2bsoftware")
        search_input.send_keys(Keys.ENTER)
        time.sleep(5)  # Wait for search results to load
        
        # Extract tweets
        tweets = []
        last_height = driver.execute_script("return document.documentElement.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(3)
            new_height = driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        print("Scraping tweets")
        tweet_elements = driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweet"]')
        for tweet in tweet_elements:
            # print("Tweets",tweet.text)
            tweets.append(tweet.text)

        # Printing first 5 tweets for brevity
        for tweet in tweets[:]:
            # print(tweet)
            producer.send('x-llm', tweet)
            producer.flush()

    except Exception as e:
        # print(f"Error in scrape_twitter: {e}")
        pass
    finally:
        driver.quit()

def betalist_scraper(producer):
    topics_data = []
    print("Scraping Betalist")
    def scrape_and_print_all_details(base_url, href_list, producer):
        for href in href_list:
            full_url = urljoin(base_url, href)
            attempt_count = 0
            while attempt_count < 3:  # Retry up to 3 times
                try:
                    # Send a GET request to the full URL
                    response = requests.get(full_url)
                    response.raise_for_status()  # Raise an HTTPError for bad responses
                    break
                except requests.RequestException as e:
                    attempt_count += 1
                    # print(f"Failed to fetch {full_url}, attempt {attempt_count}. Error: {e}")
                    time.sleep(2)  # Wait 2 seconds before retrying
            if attempt_count == 3:
                # print(f"Failed to process {full_url} after multiple attempts.")
                continue 

            # Parse the HTML content of the page
            soup = BeautifulSoup(response.text, 'html.parser')
            details_divs = soup.find_all('div', class_='startupCard__details')
            startups = []
            for div in details_divs:
                name_tag = div.find('a', class_='block whitespace-nowrap text-ellipsis overflow-hidden font-medium')
                name = name_tag.text.strip() if name_tag else "Name not found"
                href = name_tag.get('href') if name_tag else "Href not found"
                description_div = div.find('a', class_='block text-gray-500 dark:text-gray-400')
                description = description_div.get_text(strip=True) if description_div else "Description not found"
                
                startups.append({"name": name, "href": href})
                message = {'name': name, 'description': description, 'href': href}
                
                producer.send('Software', message)
                producer.flush()

            topics_data.append({"link_topic": full_url, "startups": startups})
        
        # Writing data to JSON file
        try:
            with open('output_sites2.json', 'w') as f:
                json.dump(topics_data, f, indent=4)
        except IOError as e:
            # print(f"Error writing to file: {e}")
            pass

    url = "https://betalist.com/topics"
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        containers = soup.find_all('div', class_='myContainer')
        if len(containers) >= 2:
            container = containers[1]
            links = container.find_all('a', class_='flex items-center gap-1 px-2 hover:bg-gray-100 group gap-4 hover:-my-[1px]')
            href_list = [link.get('href') for link in links]
            scrape_and_print_all_details("https://betalist.com", href_list, producer)
        else:
            # print("There are less than two 'myContainer' divs on the page.")
            pass
    except requests.RequestException as e:
        # print(f"Failed to load page {url}: {e}")
        pass


def scrape_techpoint(producer):

    print("Scraping Techpoint Africa")
    def news_producer(url_queue):
        driver = setup_webdriver()
        main_url = 'https://techpoint.africa/'
        driver.get(main_url)
        wait = WebDriverWait(driver, 10)
        attempts = 0
        max_attempts = 3

        try:
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)  # Allow time for page to load and "Load More" button to appear
                article_links = driver.find_elements(By.CSS_SELECTOR, "div.ct-div-block a.post-excerpt")
                for link in article_links:
                    href = link.get_attribute('href')
                    if href:
                        url_queue.put(href)  # Add the URL to the queue

                try:
                    attempts = 0 
                    load_more_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Load More')]")))
                    driver.execute_script("arguments[0].scrollIntoView();", load_more_button)
                    load_more_button.click()
                     # Reset attempts after a successful click
                    time.sleep(3)  # Allow time for new content to load
                except Exception as e:
                    attempts += 1
                    # print(f"Attempt {attempts}: Failed to find 'Load More' button oin techpoint. Trying again...")
                    if attempts >= max_attempts:
                        print("No more 'Load More' button found after multiple attempts. Ending production.")
                        break
                    time.sleep(3)  # Wait before trying to find the 'Load More' button again
        finally:
            driver.quit()
            url_queue.put(None)  # Signal that no more URLs will be produced

    def consumer(url_queue):
        processed_urls = set()
        while True:
            url = url_queue.get()
            if url is None:
                url_queue.task_done()
                break  # If None is fetched, no more URLs to process
            if url not in processed_urls:
                try:
                    response = requests.get(url)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    ct_div_blocks = soup.find_all('div', class_='ct-div-block')
                    data_list = []
                    h1s = soup.find_all('h1')  # Fetching all h1 tags
                    for h1 in h1s:
                        data_list.append(h1.text.strip())  # Adding h1 text to the data list
                    for block in ct_div_blocks:
                        ps = block.find_all('p')
                        uls = block.find_all('ul')
                        for p in ps:
                            if "Next" not in p.text and "Previous" not in p.text:
                                data_list.append(p.text.strip())
                        for ul in uls:
                            lis = ul.find_all('li')
                            for li in lis:
                                if "Next" not in li.text and "Previous" not in li.text:
                                    data_list.append(li.text.strip())
                    data_string = " ".join(data_list)  # Combine list items into one string
                  
                    producer.send('news', data_string)
                    processed_urls.add(url)
                except requests.RequestException as e:
                    # print(f"Failed to retrieve {url}: {str(e)}")
                    continue
                url_queue.task_done()



    url_queue = Queue(maxsize=50)  # Limit queue size if memory management is a concern
    producer_thread = threading.Thread(target=news_producer, args=(url_queue,))
    consumer_thread = threading.Thread(target=consumer, args=(url_queue,))
    
    producer_thread.start()
    consumer_thread.start()
    
    producer_thread.join()
    url_queue.join()  # Ensure that all URLs are processed
    consumer_thread.join()

def run_safe(func, *args):
    try:
        func(*args)
    except Exception as e:
        # print(f"Error in {func.__name__}: {e}")
        pass


def main():
    producer = setup_kafka_producer()
    # Initialize and start threads

    thread1 = Thread(target=run_safe, args=(scrape_slashdot, producer))
    thread2 = Thread(target=run_safe, args=(scrape_producthunt, producer))
    thread3 = Thread(target=run_safe, args=(scrape_sideprojectors, producer))
    thread4 = Thread(target=run_safe, args=(scrape_twitter, producer))
    thread5 = Thread(target=run_safe, args=(betalist_scraper, producer))
    thread6 = Thread(target=run_safe, args=(scrape_techpoint, producer))
    
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()

    # Wait for both threads to complete
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()

    producer.close()

if __name__ == "__main__":
    main()
