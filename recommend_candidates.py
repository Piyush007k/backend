import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
from joblib import Parallel, delayed
import parse_jd as parse
import json
import joblib
import os 
os.environ["TOKENIZERS_PARALLELISM"] = "false"
#Load pre-trained embedding model (BERT-based)
model = SentenceTransformer("paraphrase-MiniLM-L6-v2")
System_Prompt_summary = '''
Task: Analyse Job Description and Employee resume and give 5 points summary as why this candidate is suitable for the given job role.

Objective: As a seasoned resume analysis expert, your responsibility is to examine resumes and examine Job Description and give 5 points summary as why this candidate is suitable for the given job role.

Output Format: Provide the results in JSON format using the structure below: Do not give any other information in the output.
  {
    "Name": "Candidate's Full Name",
    "Summary":"5 Points Summary"
  }
 
Example Output:
  {
    "Name": "Shriraj Pathak",
    "Summary":"5 Points Summary"
  }
'''
def read_files(path):
    candidates_df = pd.read_excel("./static/jd_skills/employee_skill_matrix.xlsx")
    jd_df = pd.read_excel(path)

    return candidates_df, jd_df

def find_similar_skill_2(current_skill,emp_skills_list,faiss_index,emp_skill_matrix):
        print('skill not in emp sk list ->',current_skill)
        new_address_embedding = model.encode([current_skill])
        new_address_embedding_np = np.array(new_address_embedding)
        # Perform nearest neighbor search
        k = 100  # Number of nearest neighbors to retrieve
        distances, indices = faiss_index.search(new_address_embedding_np, k)

        for idx, distance in zip(indices.flatten(), distances.flatten()):
                #print(emp_skill_matrix[idx],distance)
                if emp_skill_matrix[idx] in emp_skills_list and distance <= 80:
                        print(emp_skill_matrix[idx],distance)

                        return emp_skill_matrix[idx]
def generate_summary(top_candidates_df,path,i):
    #print(top_candidates_df.Name[i])

    resume_title = top_candidates_df.Resume_Title[i]
    #print(resume_title)
    jd_title_name = 'SRE Cloud Native.pdf'

    top_resume_pdf_path = f'./static/Data_Resumes_PDF/{resume_title}'
    #jd_path_new = path
    #print(path)
    current_jd = pd.read_excel(path)
    #print(current_jd)
    jd_title_name = current_jd['JD_PDF_Title'][0]
    jd_path_new = f'./static/pdf/{jd_title_name}'

    top_cand_text = parse.get_text(top_resume_pdf_path)
    jd_text = parse.get_text(jd_path_new)
    summ_response = parse.get_response(System_Prompt_summary,f'This is the Job Description Text:\n\n{jd_text}\nThis is the text of resume:{top_cand_text}')
    sum = json.loads(summ_response)
    cand_summary = sum['Summary']
    #print('\n'.join(cand_summary)) 
    #top_candidates_df.loc[top_candidates_df['Name'] == top_candidates_df.Name[i], 'Summary'] = '\n'.join(cand_summary)
    res_df = pd.DataFrame([top_candidates_df.Name[i],'\n'.join(cand_summary)]).T
    res_df.columns = ['Name','Comments']
    return res_df

def main(path,requisition_id):
    candidates_df, jd_df = read_files(path)
    ###########################
    new_candidates_df = candidates_df.copy()
    new_candidates_df.Technology = new_candidates_df.Technology.apply(lambda x:x.lower())
    emp_skill_matrix = new_candidates_df.Technology.unique().tolist()
    ###########################

    experience = jd_df['Years_of_Experience_required'].iloc[0]

    candidates_df.Technology = candidates_df.Technology.apply(lambda x:x.lower())
    jd_df.Technology = jd_df.Technology.apply(lambda x:x.lower())

    required_technology = jd_df.Technology.to_list()
    candidates_df = candidates_df[candidates_df.Technology.str.contains('|'.join(required_technology))].reset_index(drop=True)

    experience_filter_df = candidates_df[candidates_df['Total_Experience_in_years'] >= experience]

    all_data = pd.DataFrame()
    for group_name, group_data in experience_filter_df.groupby(['Name', 'Total_Experience_in_years']):
        op = group_data[['Technology','Rating']].T
        op.columns = op.iloc[0]
        op.drop('Technology',inplace=True)
        op['Name'] = group_name[0]
        all_data = pd.concat([all_data,op],ignore_index=True)
        
    ddd = pd.DataFrame(columns=['Name'] + required_technology)
    res = pd.concat([ddd,all_data]).fillna(0)[['Name'] + required_technology]
        
    top_N_skills = 5
    filtered_df = res.iloc[:, :top_N_skills+1]
    ###################################

    skills_embeddings = model.encode(emp_skill_matrix)
    skills_embeddings_np = np.array(skills_embeddings)

    # Set up FAISS for nearest neighbor search
    dimension = skills_embeddings_np.shape[1]
    faiss_index = faiss.IndexFlatL2(dimension)
    faiss_index.add(skills_embeddings_np)

    ndf = filtered_df.copy()
    cdf = new_candidates_df.copy()
    final_final_df = pd.DataFrame()
    for emp in ndf.Name.unique():
        print(emp)
        # find emp from final skill matrx 
        gau_df = ndf[ndf['Name']==emp]

        # find columns with 0
        mask = gau_df == 0
        columns_with_zero = gau_df.columns[mask.any()]

        # find emp skills from overall emp skill matrix
        emp_skills_df = cdf[cdf['Name']==emp]
        emp_skills_list = emp_skills_df.Technology.to_list()

        # iterate through employee skill which is 0 
        for i in columns_with_zero.to_list():
            # find the similar skill
            sim_skill = find_similar_skill_2(i,emp_skills_list,faiss_index,emp_skill_matrix)
            # if similar skill value is not 0 and is present in employee skills then go in if loop
            if sim_skill not in columns_with_zero and sim_skill in emp_skills_list:
                # replace with similar skill rating
                gau_df[i]=emp_skills_df[emp_skills_df.Technology==sim_skill]['Rating'].values[0]
        final_final_df = pd.concat([final_final_df,gau_df])

    #print(final_final_df)
    numeric_columns = final_final_df.select_dtypes(include=['number']).columns
    final_final_df[numeric_columns] = final_final_df[numeric_columns] / 2

    #final_final_df['Overall'] = final_final_df.iloc[:,1:].mean(axis=1).round(2)/10*top_N_skills
    final_final_df['Overall'] = final_final_df.iloc[:,1:].mean(axis=1).round(2)#/10*top_N_skills
    final_final_df.sort_values('Overall',ascending=False,ignore_index=True,inplace=True)
    filtered_df = final_final_df.copy()

    ###### code to add summary in top capdidates #######
    top_candidates_list  =filtered_df.Name.to_list()[:2]
    #top_candidates_list

    top_candidates_df = candidates_df[candidates_df.Name.isin(top_candidates_list)][['Name','Resume_Title']].drop_duplicates().reset_index(drop=True)
    #top_candidates_df

    # for i in range(len(top_candidates_df)):
    #     print(top_candidates_df.Name[i])

    #     resume_title = top_candidates_df.Resume_Title[i]
    #     print(resume_title)
    #     jd_title_name = 'SRE Cloud Native.pdf'

    #     top_resume_pdf_path = f'./static/Data_Resumes_PDF/{resume_title}'
    #     #jd_path_new = path
    #     #print(path)
    #     current_jd = pd.read_excel(path)
    #     #print(current_jd)
    #     jd_title_name = current_jd['JD_PDF_Title'][0]
    #     jd_path_new = f'./static/pdf/{jd_title_name}'

    #     top_cand_text = parse.get_text(top_resume_pdf_path)
    #     jd_text = parse.get_text(jd_path_new)
    #     summ_response = parse.get_response(System_Prompt_summary,f'This is the Job Description Text:\n\n{jd_text}\nThis is the text of resume:{top_cand_text}')
    #     sum = json.loads(summ_response)
    #     cand_summary = sum['Summary']
    #     print('\n'.join(cand_summary)) 
    #     top_candidates_df.loc[top_candidates_df['Name'] == top_candidates_df.Name[i], 'Summary'] = '\n'.join(cand_summary)
    delayed_funcs1 = [delayed(generate_summary)(top_candidates_df,path,i) for i in range(len(top_candidates_df))]
    parallel_pool = Parallel(n_jobs=joblib.cpu_count())
    output_summary = parallel_pool(delayed_funcs1)
    summary_df = pd.concat(output_summary)

    # filtered_df = filtered_df.merge(top_candidates_df[['Name','Summary']],'left','Name')
    filtered_df = filtered_df.merge(summary_df,'left','Name')
    

    ###########################

    # capitalize
    filtered_df['Name'] = filtered_df['Name'].apply(lambda x: ' '.join(word.capitalize() for word in x.split()))
    filtered_df.columns = [col.capitalize() for col in filtered_df.columns]

    filtered_df.fillna('',inplace=True)
    print(filtered_df)

    filtered_df.to_excel(f'./static/Skill_matrix_as_per_JD/Skill_matrix_as_per_JD_{requisition_id}.xlsx',index=False)
    ###################################
    data_dict = []
    for i in range(len(filtered_df)):
        one_dict = {}
        one_dict = filtered_df.iloc[i].to_dict()
        one_dict['id'] = i+1
        data_dict.append(one_dict)
    #print(data_dict)
    print('data sent to ui')
    return data_dict


# if __name__ == '__main__':
#     main(890)

