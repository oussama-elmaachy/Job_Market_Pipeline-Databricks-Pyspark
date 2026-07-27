import requests

class api_job_search:


  def __init__(self,api_token, url,api_host,job_title):
    self.api_token = api_token
    self.url = url
    self.api_host = api_host
    self.job_title = job_title
    
  def get_jobs(self,api_token, url,api_host,job_title):
      
    headers={
                "x-rapidapi-key": api_token,
                "x-rapidapi-host": api_host 
            }
    querystring = {
                    "query":job_title,
                    "page":"1",
                    "num_pages":"50",
                    "country":"fr",
                    "date_posted":"all"
                   }
    try :
        response= requests.get(url, headers=headers, params=querystring)
        results=response.json()
        raw_data=results['data']
        return raw_data
    except Exception as e:
        print("Error occurred while fetching data from API:", e)
        return None

