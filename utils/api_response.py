import requests

class api_job_search:
  def __init__(self,token,url,host):
    self.api_token = token
    self.api_url = url
    self.api_host = host
    
  def get_jobs(self,job_title,country,date_posted):
      
    headers={
                "x-rapidapi-key": self.api_token,
                "x-rapidapi-host": self.api_host 
            }
    querystring = {
                    "query":job_title,
                    "page":"1",
                    "num_pages":"50",
                    "country":country,
                    "date_posted":date_posted
                   }
    try :

        response= requests.get(self.api_url, headers=headers, params=querystring)
        results=response.json()
        raw_data=results['data']
        return raw_data
      
    except Exception as e:

        print("Error occurred while fetching data from API:", e)
        return None
