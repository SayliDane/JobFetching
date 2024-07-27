import re
import json
import requests
import csv
from sklearn.feature_extraction.text import TfidfVectorizer
import pymongo

# API credentials and URL
app_id = "89ba7f4f"
app_key = "e4f586d25330108c3a65b498ef040b9c"
api_url = "https://api.adzuna.com/v1/api/jobs"

# MongoDB connection details
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
user_db = mongo_client['user_database']
user_collection = user_db['user_collection']
user_config_collection = user_db['user_config_collection']
jobsapplied_collection = user_db['jobsapplied']
job_collection = user_db['job']
user_preference_collection = user_db['user_preference']

# Function to search jobs using Adzuna API
def search_jobs(query, location, country, results_per_page=100, max_page=5):
    jobs = []
    for page in range(1, max_page + 1):
        endpoint = f"{api_url}/{country}/search/{page}"
        params = {
            'app_id': app_id,
            'app_key': app_key,
            'results_per_page': results_per_page,
            'what': query,
            'where': location
        }
        response = requests.get(endpoint, params=params)

        if response.status_code == 200:
            try:
                data = response.json()
                jobs.extend(data['results'])
            except json.JSONDecodeError:
                print(f"JSON decode error for page {page}")
                continue
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break
    return jobs

# Function to extract domain from company name
def extract_domain(company_name):
    company_name = company_name.replace(" ", "").lower()
    return f"{company_name}.com"

# Function to extract email from job description
def extract_email(description):
    match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', description)
    return match.group(0) if match else None

# Function to extract salary range from job description
def extract_salary(description):
    match = re.search(r'(\d+\s*-\s*\d+\s*(?:lakh|lakhs|lpa|per annum))', description, re.IGNORECASE)
    return match.group(0) if match else None

# Function to extract experience in years from job description
def extract_experience(description):
    matches = re.findall(r'(\d+)\s*(?:years|yrs|year|yr)', description, re.IGNORECASE)
    if matches:
        matches = list(map(int, matches))
        min_exp = min(matches)
        max_exp = max(matches)
        return f"{min_exp}-{max_exp} years" if min_exp != max_exp else f"{min_exp} years"
    return None

# Function to extract important keywords from job description
def extract_keywords(description, num_keywords=300):
    vectorizer = TfidfVectorizer(stop_words='english', max_features=num_keywords)
    tfidf_matrix = vectorizer.fit_transform([description])
    feature_names = vectorizer.get_feature_names_out()
    return ', '.join(feature_names)

# Function to extract job type from job description
def extract_job_type(description):
    job_types = ['full-time', 'part-time', 'internship']
    for job_type in job_types:
        if re.search(job_type, description, re.IGNORECASE):
            return job_type.capitalize()
    return None

# Function to get user subscription details from MongoDB
def get_subscription_details(user_id):
    user = user_collection.find_one({"user_id": user_id})
    if user:
        return user.get('subscription_status'), user.get('subscription_plan')
    return None, None

# Function to get user preferred data from MongoDB
def get_user_preferences(user_id):
    user_config = user_config_collection.find_one({"user_id": user_id})
    if user_config:
        return user_config.get('preferred_job_roles', []), user_config.get('preferred_location')
    return [], None

# Function to process job data and save results to MongoDB
def process_jobs(preferred_job_roles, preferred_location, country, max_page):
    all_jobs = []

    for role in preferred_job_roles:
        jobs = search_jobs(role, preferred_location, country, results_per_page=100, max_page=max_page)
        all_jobs.extend(jobs)

    if not all_jobs:
        print("No jobs found.")
        return

    job_list = []
    for job in all_jobs:
        description = job['description']
        job_info = {
            'Job Title': job.get('title', 'N/A'),
            'Company': job.get('company', {}).get('display_name', 'N/A'),
            'Location': job.get('location', {}).get('display_name', 'N/A'),
            'Description': description,
            'Company Domain': extract_domain(job.get('company', {}).get('display_name', 'N/A')),
            'Email': extract_email(description),
            'Salary Range': extract_salary(description),
            'Experience': extract_experience(description),
            'Keywords': extract_keywords(description),
            'Job Type': extract_job_type(description)
        }
        job_list.append(job_info)

    # Insert the matched jobs into the MongoDB collection
    if job_list:
        jobsapplied_collection.insert_many(job_list)
        print(f"Job data saved to MongoDB collection 'jobsapplied'.")
    else:
        print("No jobs matched the user preferences or no jobs to save based on the subscription plan.")

# Function to read job data from MongoDB
def read_jobs_from_mongodb():
    job_cursor = job_collection.find()
    job_list = list(job_cursor)
    return job_list

# Function to get user preferences from MongoDB
def get_user_preferences_for_matching(user_id):
    user_preferences = user_preference_collection.find_one({"user_id": user_id})
    return user_preferences

# Function to tokenize text and filter out numeric tokens
def custom_tokenizer(text):
    tokens = re.findall(r'\b\w+\b', text)
    tokens = [token for token in tokens if not token.isdigit()]
    return tokens

# Function to calculate skill match score between user skills and job description
def skill_match_score(user_skills, job_keywords):
    job_tokens = set(custom_tokenizer(job_keywords.lower()))
    skill_match_count = sum(skill.lower() in job_tokens for skill in user_skills)
    return skill_match_count

# Function to check if job matches user preferences
def job_matches_user_preferences(job, user_preferences):
    match_count = 0

    # Check job type
    if job.get('Job Type') == user_preferences.get('preferred_job_type'):
        match_count += 1

    # Check experience
    job_experience = job.get('Experience')
    user_experience = user_preferences.get('experience_in_years')
    if job_experience and user_experience and job_experience == user_experience:
        match_count += 1

    # Check department
    if job.get('Department') == user_preferences.get('preferred_department'):
        match_count += 1

    # Check salary
    job_salary = job.get('Salary Range')
    user_salary = user_preferences.get('expected_salary')
    if job_salary and user_salary and job_salary == user_salary:
        match_count += 1

    # Check skills
    user_skills = user_preferences.get('skills', [])
    job_keywords = job.get('Keywords', '')
    if skill_match_score(user_skills, job_keywords) > 0:
        match_count += 1

    return match_count

# Function to process and print job data based on user preferences
def process_top_matching_jobs(job_list, user_preferences):
    matched_jobs = []

    for job in job_list:
        match_count = job_matches_user_preferences(job, user_preferences)
        if match_count > 0:
            matched_jobs.append((match_count, job))

    # Sort jobs by match count in descending order
    matched_jobs.sort(reverse=True, key=lambda x: x[0])

    # Check subscription plan and determine the number of jobs to save
    subscription_plan = user_preferences.get('subscription_plan')
    if subscription_plan == 'student':
        num_jobs_to_save = 25
    elif subscription_plan == 'professional':
        num_jobs_to_save = 50
    else:
        num_jobs_to_save = 0

    # Get the top matched jobs based on the subscription plan
    top_matched_jobs = matched_jobs[:num_jobs_to_save]

    # Prepare the output for MongoDB
    output = []
    for count, job in top_matched_jobs:
        job_info = {
            'user_id': user_preferences['user_id'],  # Store user ID for reference
            'Match Count': count,
            'Job Title': job.get('Job Title', 'N/A'),
            'Company': job.get('Company', 'N/A'),
            'Location': job.get('Location', 'N/A'),
            'Description': job.get('Description', 'N/A'),
            'Company Domain': job.get('Company Domain', 'N/A'),
            'Email': job.get('Email', 'N/A'),
            'Salary Range': job.get('Salary Range', 'N/A'),
            'Experience': job.get('Experience', 'N/A'),
            'Job Type': job.get('Job Type', 'N/A')
        }
        output.append(job_info)
        print(json.dumps(job_info, indent=4))
        print("-" * 40)

    # Insert the matched jobs into the MongoDB collection
    if output:
        jobsapplied_collection.insert_many(output)
        print(f"Job data saved to MongoDB collection 'jobsapplied' for user_id {user_preferences['user_id']}")
    else:
        print("No jobs matched the user preferences or no jobs to save based on the subscription plan.")

# Main function to run both processes
def main():
    # User ID to fetch preferences and subscription details
    user_id = 'example_user_id'  # Replace with the actual user ID

    # Step 1: Fetch jobs and save to MongoDB
    subscription_status, subscription_plan = get_subscription_details(user_id)
    if subscription_status == 'active':
        if subscription_plan == 'student':
            max_page = 1  # Fetch maximum 100 jobs
        elif subscription_plan == 'professional':
            max_page = 5  # Fetch maximum 500 jobs
        else:
            print("Unknown subscription plan.")
            return

        # Get user preferred job roles and location from MongoDB
        preferred_job_roles, preferred_location = get_user_preferences(user_id)
        if preferred_job_roles and preferred_location:
            process_jobs(preferred_job_roles, preferred_location, 'US', max_page)
        else:
            print("User preferences not found.")
    else:
        print("Subscription is not active.")

    # Step 2: Get top matching jobs based on user preferences
    user_preferences = get_user_preferences_for_matching(user_id)
    if user_preferences:
        job_list = read_jobs_from_mongodb()
        process_top_matching_jobs(job_list, user_preferences)
    else:
        print("User preferences not found.")

if __name__ == "__main__":
    main()
