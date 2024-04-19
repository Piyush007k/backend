# Libraries to use
import os
import pandas as pd
import time
import json
import joblib
import pdfplumber
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime
from joblib import Parallel, delayed
import warnings
import docx2txt
warnings.filterwarnings("ignore")

# Load environment variables from .env file
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


# This is the System Prompt used as a prerequisit for generating a response.
System_Prompt = '''
Task: Extract skills, analyze work experience, and projects mentioned in the resume to rate those skills out of 10 (always make sure to not give a perfect rating i.e. 10, since no one is perfect in any technology), Rating should only contain numbers and nothing else.

Objective: As a seasoned resume analysis expert, your responsibility is to examine resumes, extract key information such as the candidate's full name, total work experience excluding internships and freelancing.
Your mission is to accurately rank skills based on the candidate's proficiency and experience level, taking into consideration both their work experience and the complexity and relevance of projects they have worked on.

Output Format: Provide the results in JSON format using the structure below: Do not give any other information in the output.
  {
    "Name": "Candidate's Full Name",
    "Total_Experience_in_years": "Years of Experience",
    "Category":"Field of Job",
    "Technology":[["Skill1","Skill Rating"],["Skill2","Skill Rating"],["Skill3","Skill Rating"]]
  }
 
Example Output:
  {
    "Name": "Shriraj Pathak",
    "Total_Experience_in_years": "3.8", 
    "Category": "Data Science",
    "Technology":[["Python","10"],["SQL","9"],["Java","8"]]
  }
 
Evaluation Criteria:
1. Correct extraction of the candidate's full name.
2. Accurate identification skills according to their expertise levels.
3. Precise calculation of the candidate's skill rating extensively based on total work experience in years and projects worked on (excluding non-professional experiences)
4. Consistent and valid JSON output format conforming to the specified structure.

Additional Guidance: When rating skills, consider the candidate's years of experience and the complexity and relevance of the projects they have completed.
A higher rating should be assigned to skills that are extensively utilized in relevant and impactful projects and if the person have high experience.
'''

def get_text(pdf_path: str)-> str:
    '''
    Function to Extract Text and Tables from the Resume PDF's
    '''
    if not pdf_path.endswith(".pdf"):
        raise ValueError("Invalid file format: Please provide a valid .pdf file.")

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

    return content

# def get_text(pdf_path: str)-> str:
#     '''
#     Function to Extract Text and Tables from the Resume PDF's
#     '''
#     print(pdf_path)
#     # if not pdf_path.endswith(".pdf") or pdf_path.endswith(".docx"):
#     #     raise ValueError("Invalid file format: Please provide a valid .pdf or .docx file.")

#     if pdf_path.endswith(".pdf"):
#         content = ""
#         try:
#             with pdfplumber.open(pdf_path) as pdf:
#                 for page in pdf.pages:
#                     extracted_text = page.extract_text()
#                     content += extracted_text

#                     # Extracting table
#                     try:
#                         tables = page.extract_tables()
#                         for idx, table in enumerate(tables):
#                             content += f"\nTable #{idx + 1}:\n"
#                             for row in table:
#                                 content += ', '.join([str(_) for _ in row]) + '\n'
#                     except Exception as e:
#                         print(f"Failed to extract table from page {page}. Error: {e}")
#         except Exception as e:
#             print(f"Error opening or reading PDF file at path '{pdf_path}'")
#             raise e
#     else:
#         content = docx2txt.process(pdf_path)
#         #print(my_text)

#     return content


def get_response(System_Prompt: str, final_resume_text: str, selected_model="gpt-4"):
    """
    Function used for generating response form OpenAI model
    Here we are Passing the System Prompt and Extracted text from resume.
    """

    client = OpenAI(api_key=OPENAI_API_KEY)
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



def response_to_df(response: str, pdf_path: str)-> pd.DataFrame:
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
    temp_df.rename(columns={0:'Technology',1:'Rating'},inplace=True)
    temp_df['Name']=api_response['Name']
    temp_df['Total_Experience_in_years']=api_response['Total_Experience_in_years']
    temp_df['Job Role']=api_response['Category']
    temp_df['Name'] = temp_df['Name'].str.lower()
    temp_df['Resume_Title'] = os.path.basename(pdf_path)
    return temp_df
    

def extract_skills(pdf_path: str, System_Prompt: str, selected_model: str)-> pd.DataFrame:
    """
    Implementing the above created functions to extract and generate required information from the resumes like skills and total experience in years. 
    """

    resume_text = get_text(pdf_path)
    response = get_response(System_Prompt, resume_text, selected_model)
    final_df = response_to_df(response,pdf_path)
    return final_df


def main(pdf_path: str, System_Prompt: str, resume_list: list[str], selected_model: str):
    """
    Main function for generating Skill_matrix Dataframe in parallel for all resumes.
    """
   
    delayed_funcs = [delayed(extract_skills)(pdf_path+filename, System_Prompt,selected_model) for filename in resume_list]
    parallel_pool = Parallel(n_jobs=joblib.cpu_count())
    output_response = parallel_pool(delayed_funcs)
    final_df1 = pd.concat(output_response)
    return final_df1.reset_index(drop=True)


def create_resume_metadata(list_of_pdf, dir_path):
    '''
    Finction to create metadata for Resumes for tracking old and new resumes.
    '''
    #creation = [datetime.fromtimestamp(os.path.getmtime(dir_path+f)) for f in list_of_pdf if f.endswith('.pdf')]
    creation = [datetime.fromtimestamp(os.path.getmtime(dir_path+f)) for f in list_of_pdf if f.endswith('.pdf')]
    title = [f for f in list_of_pdf if f.endswith('.pdf')]
 
    resume_metadata = pd.DataFrame([title,creation]).T
    resume_metadata.columns = ['Title','Creation_time']
    resume_metadata.to_excel('./static/jd_skills/resume_metadata.xlsx',index=False)
    return resume_metadata

 
def start():
    '''
    Generating Skill Matrix and the Files Metadata, so if a new file gets added into the folder
    '''
    
    # Directory path for all the resumes
    dir_path = './static/Data_Resumes_PDF/'
    print(dir_path)
    

    # selected_model = 'gpt-4-turbo-preview'
    selected_model = 'gpt-3.5-turbo'
    # selected_model = 'gpt-4'

    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

    if os.path.exists(dir_path):
        # check if metadata file is present then read that file, ie next run if new pdf added:
        if os.path.exists(f'./static/jd_skills/resume_metadata.xlsx'):
            resume_metadata = pd.read_excel('./static/jd_skills/resume_metadata.xlsx')
        
        # If absent then create it, ie initial run
        else:
            resume_metadata = create_resume_metadata(os.listdir(dir_path), dir_path)
            resume_metadata.to_excel(f'./static/jd_skills/resume_metadata.xlsx',index=False)
        
        # Creating a list containing new .pdf files:
        new_resume_list = [f for f in os.listdir(dir_path) if f.endswith('.pdf') and f not in resume_metadata.Title.unique().tolist()]
        print(len(new_resume_list))
        # If new pdf added:
        if len(new_resume_list)>0:
            print('Total number of new resumes:',len(new_resume_list))
            print('Calculating skill matrix of new resumes.')
            # read og skill matrix
            skill_matrix_og = pd.read_excel('./static/jd_skills/employee_skill_matrix.xlsx')
            # calculate skill matrix for new pdf list
            skill_matrix = main(dir_path,System_Prompt,new_resume_list,selected_model)
            # concate new resumes
            new_skill_matrix = pd.concat([skill_matrix_og,skill_matrix]).reset_index(drop=True)
            # save new skill matrix
            new_skill_matrix.to_excel('./static/jd_skills/employee_skill_matrix.xlsx',index=False)
    
            # calculate and append new metadata with old one
            resume_metadata_new = create_resume_metadata(new_resume_list, dir_path)
            resume_metadata_all = pd.concat([resume_metadata,resume_metadata_new]).reset_index(drop=True)
            resume_metadata_all.to_excel('./static/jd_skills/resume_metadata.xlsx',index=False)

        # Else loop for initial run
        else:
            if os.path.exists(f'./static/jd_skills/employee_skill_matrix.xlsx')==False:
                print('Initial Run. Calculating skill matrix of all resumes.')
                print(os.listdir(dir_path),'&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
                #try:
                skill_matrix = main(dir_path,System_Prompt,[f for f in os.listdir(dir_path) if f.endswith('.pdf')],selected_model)
                skill_matrix.to_excel('./static/jd_skills/employee_skill_matrix.xlsx',index=False)
                print('Skill Matrix created')
                #except:
                    #print('Please Upload Resumes First.')
                    #return 'Please Upload Resumes First.'
    else:
        print(f"The specified directory '{dir_path}' does not exist. Please provide a valid path.")


#start()
