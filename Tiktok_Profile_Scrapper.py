#import libraries
import time, os, psycopg2, json
from datetime import datetime
import pandas as pd

#import requests
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

#import fake user agent
from fake_useragent import UserAgent

#import google cloud
from google.cloud import storage
from google.oauth2 import service_account

#import from flask
from flask import Flask, request
from flask_cors import CORS
from flask_restful import reqparse

#hyper corn asyncio setup
import asyncio
from hypercorn.config import Config
from hypercorn.asyncio import serve
from asgiref.wsgi import WsgiToAsgi

#import libraries
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy,ProxyType

#import beautiful soup
from bs4 import BeautifulSoup

#initialize the defaults
project_id = 'data-collection-366512'
platform = "Tiktok"
host = "34.70.83.114"
user = "postgres"
password = 'tL("JG:Mf>G/UL*a'
database = "postgres"
bucket_name = 'csv-uploads-vn-data-collection'
data_type = 'Profile'

#uncomment if running on locally and comment server part
#GCP bucket defaults
with open('data-collection-366512-63872a5e3da3.json') as source:
            info = json.load(source)

#create a service for GCP
storage_credentials = service_account.Credentials.from_service_account_info(info)

#initialize the client service
storage_client = storage.Client(project=project_id, credentials=storage_credentials)

#initialize the bucket
bucket = storage_client.get_bucket(bucket_name)

#hyper corn default config
config = Config()
config.bind = ["0.0.0.0:2019"]
config.keep_alive_timeout = 5000
config.graceful_timeout = 5000
config.websocket_ping_interval = 1
config.worker_class = "asyncio"

#flask default config
app = Flask(__name__)
CORS(app)
WsgiToAsgi(app)

#generate proxy ip address
ip = '{}.{}.{}.{}'.format(*__import__('random').sample(range(0,255),4))
p = '{}'.format(*__import__('random').sample(range(1000,8000),1))
ip=ip+':'+p

# Configure Proxy Option
prox = Proxy()
prox.proxy_type = ProxyType.MANUAL

# Proxy IP & Port
prox.http_proxy = ip
prox.https_proxy = ip

# Configure capabilities
capabilities = webdriver.DesiredCapabilities.CHROME
prox.add_to_capabilities(capabilities)
option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-extensions')
option.add_argument('--disable-dev-shm-usage')
option.add_argument('--ignore-certificate-errors')
option.add_argument('--disable-gpu')
option.add_argument('--log-level=3')
option.add_argument('--incognito')
option.add_argument('--disable-notifications')
option.add_argument('--disable-popup-blocking')
option.add_argument("enable-automation")
option.add_argument("--disable-infobars")

#load the chrome driver
#driver = webdriver.Chrome("/Users/devanshmody/Downloads/chromedriver",desired_capabilities=capabilities,chrome_options=option)
driver = webdriver.Chrome(executable_path="/usr/local/bin/chromedriver",desired_capabilities=capabilities,chrome_options=option)

#decorator function for calculating the total time required to execute various functions
def calc_time(func):
    def inner(*args, **kwargs):
        st = time.time()
        result = func(*args, **kwargs)
        end = time.time()-st
        print("Total time required for function {}: {:.3f} ms".format(func.__name__,end * 1000))
        return result
    return inner

#function to retry to get the required data from the website
def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(429, 500, 502, 503, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

#function for one liner try except block
def safe_execute1(i1,i2,i3,data):

    try:

        op = data.get("{}".format(i1),None).get("{}".format(i2),None).get("{}".format(i3),None)
        return op

    except Exception as e:
        print(e)
        return None

#function for one liner try except block
def safe_execute2(i1,i2,i3,i4,data):

    try:

        op = data.get("{}".format(i1),None).get("{}".format(i2),None).get("{}".format(i3),None).get("{}".format(i4),None)
        return op

    except Exception as e:
        print(e)
        return None

#function for one liner try except block
def safe_execute3(i1,i2,data):

    try:

        op = data.get("{}".format(i1),None).get("{}".format(i2),None)
        return op

    except Exception as e:
        print(e)
        return None

#update the postgres logs
@calc_time
def update_log(_id, social_handle, status, result_path, body, access_token, service_type):

    try:
        #establish connection to postgres table
        connection = psycopg2.connect(
            host = host,
            user = user,
            password = password,
            database = database
        )
        cursor = connection.cursor()
        if status == "Started":

            #insert entry into postgres database
            postgres_insert_query = "INSERT INTO collection_logs (collection_id, started_log, start_time, platform, social_handle, access_token, service_type) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            record_to_insert = (_id, status, str(datetime.utcnow()), platform, social_handle, access_token, service_type)
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into collection_logs")

        if status == "Completed":
            json_object = json.dumps(body)
            #update entry into postgres database
            cursor.execute("UPDATE collection_logs SET completed_log = '{}', path_to_results = '{}', end_time = '{}', response_body = '{}'  WHERE collection_id = {}".format(status, result_path, str(datetime.utcnow()), json_object, _id))
            connection.commit()
            print("Record inserted successfully into collection_logs")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into collection_logs", error)

#get the values from postgres table and update the values and create the csv file to store in bucket
@calc_time
def get_postgres_values(social_handle, df, access_token):

    try:
        print("comes here")
        #establish connection to postgres table
        connection = psycopg2.connect(
            host = host,
            user = user,
            password = password,
            database = database
        )
        cursor = connection.cursor()

        #insert entry into postgres database
        postgres_insert_query = "INSERT INTO user_collections (social_handle, platform, date_time, access_token) VALUES (%s,%s,%s,%s) returning id"
        record_to_insert = (social_handle, platform,str(datetime.utcnow()),access_token)
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
        _id = int(cursor.fetchone()[0])
        print("the id received",_id)
        count = cursor.rowcount
        print(count, "Record inserted successfully into user_collections")

        #if dataframe
        if isinstance(df, pd.DataFrame):

            print("generate csv and store for post id")

            #generate csv path to store in postgres
            csv_path = f'{_id}/CSV_ValidateHandle/{platform}_{data_type}.csv'

            #generate temporary files
            df.to_csv(f'{platform}_{data_type}.csv',index=False)

            #upload csv file
            blob = bucket.blob(csv_path)
            blob.upload_from_filename(f'{platform}_{data_type}.csv')

            #remove the temporary files
            os.remove(f'{platform}_{data_type}.csv')

            cursor.execute("UPDATE user_collections SET path_to_csv = '{}' WHERE id = {}".format(csv_path,_id))
            connection.commit()

            print("the last entry",_id)

            return csv_path, _id

    except Exception as e:
        print("Failed to insert record into user_collections", e)

#function to check if access token is present or not
@calc_time
def check_token_present(access_token):

    try:
        #establish connection to postgres table
        connection = psycopg2.connect(
            host = host,
            user = user,
            password = password,
            database = database
        )
        cursor = connection.cursor()

        #search if record exists in database
        postgres_search_query = f"SELECT * FROM users WHERE access_token = '{access_token}'"
        cursor.execute(postgres_search_query)
        connection.commit()
        res = cursor.fetchone()[3]

        if len(res)!=0:
            print("Token Validation Success")
            return True

    except (Exception, psycopg2.Error) as error:
        print("Token Not Found", error)

        return False

#get the driver
'''
@calc_time
def get_driver():

    #generate proxy ip address
    ip = '{}.{}.{}.{}'.format(*__import__('random').sample(range(0,255),4))
    p = '{}'.format(*__import__('random').sample(range(1000,8000),1))
    ip=ip+':'+p

    # Configure Proxy Option
    prox = Proxy()
    prox.proxy_type = ProxyType.MANUAL

    # Proxy IP & Port
    prox.http_proxy = ip
    prox.https_proxy = ip

    # Configure capabilities
    capabilities = webdriver.DesiredCapabilities.CHROME
    prox.add_to_capabilities(capabilities)
    option = webdriver.ChromeOptions()
    option.add_argument('--headless')
    option.add_argument('--no-sandbox')
    option.add_argument('--disable-extensions')
    option.add_argument('--disable-dev-shm-usage')
    option.add_argument('--ignore-certificate-errors')
    option.add_argument('--disable-gpu')
    option.add_argument('--log-level=3')
    option.add_argument('--incognito')
    option.add_argument('--disable-notifications')
    option.add_argument('--disable-popup-blocking')
    option.add_argument("enable-automation")
    option.add_argument("--disable-infobars")

    #load the chrome driver
    driver = webdriver.Chrome("/Users/devanshmody/Downloads/chromedriver",desired_capabilities=capabilities,chrome_options=option)
    #driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver',desired_capabilities=capabilities,chrome_options=option)

    return driver
'''
#get tiktok profile data using selenium
@calc_time
def get_tiktok_profile_selenium(social_handle,access_token):

    try:

        #driver = get_driver()

        #get tiktok account
        driver.get(f"https://www.tiktok.com/@{social_handle}")

        #load the data
        soup = BeautifulSoup(driver.page_source, 'lxml')

        #print(soup)

        #check if empty or not
        if len(soup)!=0:

            #filter to get profile data
            op = soup.find("script",{"id":"SIGI_STATE"}).contents
            op = json.loads(op[0])
            op = op["UserModule"]

            print("sending user profile information")
            print(op)
            #get the required profile data
            user_profile = {
                'social_user_id':str(safe_execute1("users",social_handle,"id",op)),
                'shortId':str(safe_execute1("users",social_handle,"shortId",op)),
                'username':str(safe_execute1("users",social_handle,"uniqueId",op)),
                'nickname':str(safe_execute1("users",social_handle,"nickname",op)),
                'profileLarger':str(safe_execute1("users",social_handle,"avatarLarger",op)),
                'profileMedium':str(safe_execute1("users",social_handle,"avatarMedium",op)),
                'profileThumbnail':str(safe_execute1("users",social_handle,"avatarThumb",op)),
                'signature':str(safe_execute1("users",social_handle,"signature",op)),
                'is_verified':True if str(op.get("users",None).get(f"{social_handle}",None).get("verified",None)) == "True" else False,
                'secUid':str(safe_execute1("users",social_handle,"secUid",op)),
                'bioLink':str(safe_execute2("users",social_handle,"bioLink",'link',op)),
                'is_private':True if str(op.get("users",None).get(f"{social_handle}",None).get("privateAccount",None)) == "True" else False,
                'followers':int(safe_execute1("stats",social_handle,"followerCount",op)),
                'following':int(safe_execute1("stats",social_handle,"followingCount",op)),
                'heartCount':int(safe_execute1("stats",social_handle,"heartCount",op)),
                'total_post':int(safe_execute1("stats",social_handle,"videoCount",op)),
                'diggCount':int(safe_execute1("stats",social_handle,"diggCount",op)),
            }

            #create a dataframe
            df = pd.DataFrame(user_profile,index=[0])

            #store information in logs
            csv_path, _id = get_postgres_values(social_handle,df,access_token)

            #update logs
            update_log(_id, social_handle, status="Started", result_path=None, body=None, access_token=access_token, service_type = f"DataCollection_{data_type}")
            update_log(_id, social_handle, status="Completed", result_path=csv_path, body=user_profile, access_token = access_token, service_type = None)

            #add collection id
            user_profile.update({"collection_id":_id})

            #driver.quit()

            return user_profile,200

        else:
            return {"message":"account does not exists"},404

    except Exception as e:
        print(e)
        return {"message":"bad request"},404


#validate the handle part 2
@calc_time
def get_info_without_selenium(social_handle,access_token):

    try:

        #generate proxy ip address
        ip = '{}.{}.{}.{}'.format(*__import__('random').sample(range(100,255),4))
        p = '{}'.format(*__import__('random').sample(range(1000,8000),1))
        ip=ip+':'+p

        #rotate user agents
        ua=UserAgent()
        headers = {'User-Agent':ua.random}

        #rotate proxies
        proxies = {"http://": "https://"+ip,}

        #get the profile data
        url =f"https://www.tiktok.com/api/user/detail/?uniqueId={social_handle}&msToken=pHhICntvmREGxjQRm0DV6ydan0yTzKzz4XO9cIPRt9W_F6bPTQwfrm_waRBAVvBiSXepWq5snCynLHwLIuY2MsYQdTGqdBMLrhu_vShwlyKI-wQOY3RotcSKhimS6MlzgB47doI="

        print("url: {}".format(url))

        s = requests.Session()
        #get the data
        r = requests_retry_session(session=s).get(
            url,headers=headers,proxies=proxies
        )

        if r.status_code == 404:
            return get_tiktok_profile_selenium(social_handle)
            #return {"message":"account does not exists"},404

        elif r.status_code == 200:

            #get the response body
            data = r.json()
            data = data["userInfo"]

            print("sending user profile information")

            #get the required profile data
            user_profile = {
                'social_user_id':str(safe_execute2("user","id",data)),
                'shortId':str(safe_execute2("user","shortId",data)),
                'username':str(safe_execute2("user","uniqueId",data)),
                'nickname':str(safe_execute2("user","nickname",data)),
                'profile_pic_large':str(safe_execute2("user","avatarLarger",data)),
                'profile_pic_medium':str(safe_execute2("user","avatarMedium",data)),
                'profile_pic_thumbnail':str(safe_execute2("user","avatarThumb",data)),
                'signature':str(safe_execute2("user","signature",data)),
                'is_verified':True if str(data.get("user",None).get("verified",None)) == "True" else False,
                'secUid':str(safe_execute2("user","secUid",data)),
                'bioLink':str(safe_execute1("user","bioLink","link",data)),
                'is_private': True if str(data.get("user",None).get("privateAccount",None)) == "True" else False,
                'followers':int(safe_execute2("stats","followerCount",data)),
                'following':int(safe_execute2("stats","followingCount",data)),
                'heartCount':int(safe_execute2("stats","heartCount",data)),
                'total_post':int(safe_execute2("stats","videoCount",data)),
                'diggCount':int(safe_execute2("stats","diggCount",data)),
            }

            #create a dataframe
            df = pd.DataFrame(user_profile,index=[0])

            #store information in logs
            csv_path, _id = get_postgres_values(social_handle,df,access_token)

            #update logs
            update_log(_id, social_handle, status="Started", result_path=None, body=None, access_token=access_token, service_type = f"DataCollection_{data_type}")
            update_log(_id, social_handle, status="Completed", result_path=csv_path, body=user_profile, access_token = access_token, service_type = None)

            #add collection id
            user_profile.update({"collection_id":_id})

            return user_profile,200
        else:
            return get_tiktok_profile_selenium(social_handle,access_token)

    except Exception as e:
        print(e)
        try:
            return get_tiktok_profile_selenium(social_handle,access_token)
        except Exception as e:
            return {"error":"something went wrong"},500


#validate the handle
@app.route("/tiktok_verify_handle",methods=["POST"])
def verify_request():

    time.sleep(5)

    #arguments for flask api call
    parser = reqparse.RequestParser()
    parser.add_argument('social_handle',type=str,help="social media platform handle",required=True)
    parser.add_argument('access_token',type=str,help="access token to get access to services",required=True)

    #get the payload
    args = parser.parse_args()
    social_handle = args['social_handle']
    access_token = args['access_token']

    try:

        if check_token_present(access_token) != True:
            return {"message":"Access Token Invalid"},500

        #generate proxy ip address
        ip = '{}.{}.{}.{}'.format(*__import__('random').sample(range(100,255),4))
        p = '{}'.format(*__import__('random').sample(range(1000,8000),1))
        ip=ip+':'+p

        #rotate user agents
        ua=UserAgent()
        headers = {'User-Agent':ua.random}

        #rotate proxies
        proxies = {"http://": "https://"+ip,}

        #get the profile data
        url = f'https://www.tiktok.com/@{social_handle}?verifyFp=verify_lavcu90f_OkjDEnKp_FFst_49tw_BY8r_deou8u5xAQ3X&msToken=aZFDgznNqLHcZmUZS7z3XKHz0y0PA56DApBBcWOF_c1Oeb5w0vI7AI8V32eDHAwA6nB0tGpG9Oly0sAkCfrw8T1PBSB7nfs8b1ENu8Swy6mEPVpp-008qJ2BPw4q8qUOhnT4BQ==&lang=en'
        print("url: {}".format(url))

        s = requests.Session()
        #get the data
        r = requests_retry_session(session=s).get(
            url,headers=headers,proxies=proxies
        )

        if r.status_code == 404:
            return get_info_without_selenium(social_handle,access_token)

        elif r.status_code == 200:

            #load the first handle page of user as soup
            soup = BeautifulSoup(r.content, 'lxml')

            #filter to get profile data
            op = soup.find("script",{"id":"SIGI_STATE"}).contents
            op = json.loads(op[0])
            op = op["UserModule"]

            print(op)

            #get the required profile data
            user_profile = {
                'social_user_id':str(safe_execute1("users",social_handle,"id",op)),
                'shortId':str(safe_execute1("users",social_handle,"shortId",op)),
                'username':str(safe_execute1("users",social_handle,"uniqueId",op)),
                'nickname':str(safe_execute1("users",social_handle,"nickname",op)),
                'profileLarger':str(safe_execute1("users",social_handle,"avatarLarger",op)),
                'profileMedium':str(safe_execute1("users",social_handle,"avatarMedium",op)),
                'profileThumbnail':str(safe_execute1("users",social_handle,"avatarThumb",op)),
                'signature':str(safe_execute1("users",social_handle,"signature",op)),
                'is_verified':True if str(op.get("users",None).get(f"{social_handle}",None).get("verified",None)) == "True" else False,
                'secUid':str(safe_execute1("users",social_handle,"secUid",op)),
                'bioLink':str(safe_execute2("users",social_handle,"bioLink",'link',op)),
                'is_private':True if str(op.get("users",None).get(f"{social_handle}",None).get("privateAccount",None)) == "True" else False,
                'followers':int(safe_execute1("stats",social_handle,"followerCount",op)),
                'following':int(safe_execute1("stats",social_handle,"followingCount",op)),
                'heartCount':int(safe_execute1("stats",social_handle,"heartCount",op)),
                'total_post':int(safe_execute1("stats",social_handle,"videoCount",op)),
                'diggCount':int(safe_execute1("stats",social_handle,"diggCount",op)),
            }

            #create a dataframe
            df = pd.DataFrame(user_profile,index=[0])

            #store information in logs
            csv_path, _id = get_postgres_values(social_handle,df,access_token)

            #update logs
            update_log(_id, social_handle, status="Started", result_path=None, body=None, access_token=access_token, service_type = f"DataCollection_{data_type}")
            update_log(_id, social_handle, status="Completed", result_path=csv_path, body=user_profile, access_token = access_token, service_type = None)

            #add collection id
            user_profile.update({"collection_id":_id})

            return user_profile,200
        else:
            return get_info_without_selenium(social_handle,access_token)

    except Exception as e:
        print(e)
        try:
            return get_info_without_selenium(social_handle,access_token)
        except Exception as e:
            return {"error":"something went wrong"},500

@app.route('/',methods=["GET"])
def health_check():
    """
    health_check
    GET Request
    """
    try:
        return {"message":"success"},200
    except:
        return {"message":"failed"},404

#run server
if __name__ == '__main__':

    asyncio.run(serve(app, config))
