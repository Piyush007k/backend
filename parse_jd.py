import os
import pandas as pd
import time
import json
#import joblib
import pdfplumber
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime
#from joblib import Parallel, delayed
import warnings
import docx2txt
warnings.filterwarnings("ignore")


# Load environment variables from .env file
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

System_Prompt = '''
Task: Extract Description of the job, job requirements and Qualifications from this job description document and rate the requirements out of 1 to 10 (10 being the most required skill and 1 being the least required skill and always make sure to not give a perfect rating i.e. 10), Rating should only contain numbers and nothing else.

Objective: As a seasoned job description document analysis expert, your responsibility is to examine job description for a particular position that you will find, extract key information such as the designation or role, requirements for that role and the qualifications for it.
Your mission is to accurately rank skills based on the requirements mention in the document, taking into consideration the role and the qualifications required.

Output Format: Provide the results in JSON format using the structure below: Do not give any other information in the output.
  {
    "Role": "Job role/ designation/title",
    "Years_of_Experience_required": "Years of Experience",
    "Category":"Field of Job",
    "Technology":[["Skill1","Skill Rating"],["Skill2","Skill Rating"],["Skill3","Skill Rating"]]
  }

Example Output:
  {
    "Role": "Data Scientist",
    "Years_of_Experience_required": "3+", 
    "Category": "Data Science",
    "Technology":[["Python","9"],["SQL","9"],["Java","8"]]
  }

Evaluation Criteria: 1. Correct extraction of the role/designation/title. 2. Accurate identification of required 
skills from the document. 3. Precise calculation of the the required skill rating. 4. Consistent and valid JSON 
output format conforming to the specified structure. 5. All the ratings must be relevant according to the job 
description and title (A cloud engineer must have AWS, Azure, linux ,etc rated higher whereas an machine lerning 
engineer post must have python, machine lerning, sql, etc rated highly)

Additional Guidance: When rating required skills, consider the relevancy of that skill with the job title and description.
the Years_of_Experience_required should be a number without +
Where ever you see Azure or AWS written before someting make sure to replace with Azure and or AWS
Make sure Technologies are not written together like AWS/ Azure they should be written on different rows
If total years of experience required for the job is not specified then use it as 0.

Must not do: Must not include personal skills in technical skills. For example Communication skills must not be included in Technology.
No two technologies/skills must be on the same row or comma separated or even written inside brackets they must be on separate rows all the time.'''


# def get_text(pdf_path: str) -> str:
#     '''
#     Function to Extract Text and Tables from the Resume PDF's
#     '''
#     if not pdf_path.endswith(".pdf"):
#         raise ValueError("Invalid file format: Please provide a valid .pdf file.")

#     content = ""
#     try:
#         with pdfplumber.open(pdf_path) as pdf:
#             for page in pdf.pages:
#                 extracted_text = page.extract_text()
#                 content += extracted_text
#         return content
#     except Exception as e:
#         print(f"Error opening or reading PDF file at path '{pdf_path}'")
#         raise e

def get_text(pdf_path: str)-> str:
    '''
    Function to Extract Text and Tables from the Resume PDF's
    '''
    # if not pdf_path.endswith(".pdf") or pdf_path.endswith(".docx"):
    #     raise ValueError("Invalid file format: Please provide a valid .pdf or .docx file.")

    if pdf_path.endswith(".pdf"):
        content = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    extracted_text = page.extract_text()
                    content += extracted_text

                    # Extracting table
                    try:
                        tables = page.extract_tables()
                        for idx, table in enumerate(tables):
                            content += f"\nTable #{idx + 1}:\n"
                            for row in table:
                                content += ', '.join([str(_) for _ in row]) + '\n'
                    except Exception as e:
                        print(f"Failed to extract table from page {page}. Error: {e}")
        except Exception as e:
            print(f"Error opening or reading PDF file at path '{pdf_path}'")
            raise e
    else:
        content = docx2txt.process(pdf_path)
        #print(my_text)

    return content



def get_response(System_Prompt: str, final_resume_text: str, selected_model="gpt-4-turbo-preview"):
    """
    Function used for generating response form OpenAI model
    Here we are Passing the System Prompt and Extracted text from resume.
    """
    print('Running api')

    client = OpenAI(api_key="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")
    time.sleep(1)

    if selected_model in ['gpt-4-turbo-preview',
                          'gpt-3.5-turbo',
                          'gpt-4-0125-preview',
                          'gpt-4-1106-preview',
                          'gpt-3.5-turbo-0125',
                          'gpt-3.5-turbo-1106']:
        response_format = {"type": "json_object"}
    else:
        response_format = None

    try:
        response = client.chat.completions.create(
            model=selected_model,
            messages=[
                {"role": "system", "content": System_Prompt},
                {"role": "user", "content": final_resume_text}],
            response_format=response_format,
            temperature=0
            )
    except Exception as e:
        print(f"Error creating completion request for model '{selected_model}'")
        raise e

    return response.choices[0].message.content


def response_to_df(response: str, pdf_path: str) -> pd.DataFrame:
    """
    Function used for generating DataFrame from response.
    """
    # print(response)
    try:
        api_response = json.loads(response)
    except Exception as e:
        print(f"Unable to parse API JSON response:\n{response}\nError: {e}")
        raise e
    temp_df = pd.DataFrame(api_response['Technology'])
    temp_df.rename(columns={0: 'Technology', 1: 'Rating'}, inplace=True)
    temp_df['Role'] = api_response['Role']
    temp_df['Years_of_Experience_required'] = api_response['Years_of_Experience_required']
    temp_df['Domain'] = api_response['Category']
    temp_df['Role'] = temp_df['Role'].str.lower()
    temp_df['JD_PDF_Title'] = os.path.basename(pdf_path)
    return temp_df


def extract_skills(pdf_path: str, System_Prompt: str, selected_model: str) -> pd.DataFrame:
    """
    Implementing the above created functions to extract and generate required information from the resumes like skills and total experience in years.
    """
    jd_text = get_text(pdf_path)
    response = get_response(System_Prompt, jd_text, selected_model)
    final_df = response_to_df(response, pdf_path)
    return final_df

# if __name__ == "__main__":
#     '''
#     Generating Skill Matrix and the Files Metadata, so if a new file gets added into the folder
#     '''

#     # Directory path for all the resumes
#     pdf_dir_path = r'Data_Resumes_PDF'

#     jd_dir_path = r"Data_JD/Data Science.pdf"
#     # selected_model = 'gpt-4-turbo-preview'
#     #selected_model = 'gpt-3.5-turbo'
#     selected_model = 'gpt-4'
#     current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
#     if os.path.exists(jd_dir_path):
#         skill_matrix = extract_skills(jd_dir_path, System_Prompt, selected_model)
#         skill_matrix.to_excel('jd_skill_matrix.xlsx',index=False)
#     else:
#         print(f"The specified directory '{jd_dir_path}' does not exist. Please provide a valid path.")
