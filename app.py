import requests
import pandas as pd
import re

def extract(index):
    url = "https://jobs-api14.p.rapidapi.com/v2/list"
    querystring = {"query":"Web Developer","location":"United States", "index":index}
    headers = {
	"x-rapidapi-key": "21f1d8efa0mshcd84f59d5ab28e5p1ba6ffjsn91ae6f339d9a",
	"x-rapidapi-host": "jobs-api14.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    # print(response.status_code)
    return response.json()

def fetch_all_jobs(page_num):
    all_jobs=[]
    for index in range(1, page_num+1):
        data= extract(index)
        jobs= data.get("jobs", [])
        all_jobs.extend(jobs)
    return all_jobs


def parse_description(description):
    # Default values
    parsed_data = {
        "education": None,
        "experience": None,
        "salary": None,
        "benefits": None,
    }

    # Education extraction
    education_keywords = ["Bachelor's degree", "Master's degree", "high school diploma", "college degree"]
    for keyword in education_keywords:
        if keyword.lower() in description.lower():
            parsed_data["education"] = keyword
            break

    # Experience extraction
    experience_pattern = r"(\d+\+?\s*years? of experience)"
    experience_match = re.search(experience_pattern, description, re.IGNORECASE)
    if experience_match:
        parsed_data["experience"] = experience_match.group(1)
    
    # Salary extraction
    salary_pattern = r"(\$\d+[,\d]*)"
    salary_match = re.search(salary_pattern, description)
    if salary_match:
        parsed_data["salary"] = salary_match.group(1)
    
    # Benefits extraction
    benefits_keywords = ["health insurance", "401(k)", "paid time off", "benefits"]
    benefits_found = []
    for keyword in benefits_keywords:
        if keyword.lower() in description.lower():
            benefits_found.append(keyword)
    if benefits_found:
        parsed_data["benefits"] = ", ".join(benefits_found)
 
    return parsed_data

def transform(data):
    job_details=[]
    # jobs = data.get("jobs", [])  # Get the "jobs" key, default to an empty list if not found.
    for job in data:
        description = job.get("description", "")  # Extract the description
        
        # Extract provider name and URL to apply (assuming jobProviders is a list)
        providers = job.get("jobProviders", [])
        if providers:
            provider_name = providers[0].get("jobProvider", "")
            provider_url = providers[0].get("url", "")
        else:
            provider_name = ""
            provider_url = ""
        
        parsed_details = parse_description(description)
        
        job_details.append({
            "title" : job.get("title"),  # Extract the title
            "company" : job.get("company"),  # Extract the company
            "description": description,
            "parsed_details" : parse_description(description),
            "location" : job.get("location"),  # Extract the location
            "employment_type":  job.get("employmentType"),  # Extract the employment type
            "education": parsed_details["education"],  # Education column
            "experience": parsed_details["experience"],  # Experience column
            "salary": parsed_details["salary"],  # Salary column
            "provider_name": provider_name,  # Extracted provider name
            "provider_url": provider_url   # Extracted provider URL
        })

    df = pd.DataFrame(job_details)
    return df


all_jobs_data= fetch_all_jobs(10)
df = transform(all_jobs_data)
# print(df.shape)
# # print(df)
# df.to_excel('jobs_data.xlsx', index=False, engine='openpyxl')

# print("Data saved to jobs_data.xlsx")

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

username = 'root'
password = '03102000'
db_name = 'jobsapidata'
host = 'localhost'
port = 3306

try:
    # Use MySQL Connector with timeout settings
    connection_string = f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{db_name}?charset=utf8mb4"
    engine = create_engine(connection_string, pool_recycle=3600, pool_size=10, max_overflow=20)

    # Test the connection
    with engine.connect() as connection:
        print("Connected to MySQL Server version", connection.connection.get_server_info())

except SQLAlchemyError as e:
    print("Error:", e)
