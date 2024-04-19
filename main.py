from flask import Flask, render_template, request,jsonify,make_response
import functions as f
import os
import parse_jd as parse
import main_generating_skill_matrix_task as gen_matrix
import recommend_candidates as recommend
from flask_cors import CORS
import sqlite3
app = Flask(__name__)
CORS(app)
app.secret_key = os.urandom(24)


DATABASE = 'Full_Stack_Database.db'
 
def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.execute('PRAGMA foreign_keys = ON;')
    conn.row_factory = sqlite3.Row
    return conn
 
# Create tables if they do not exist
with get_db_connection() as conn:
    conn.execute('''CREATE TABLE IF NOT EXISTS organization_table
                 (Org_id INTEGER PRIMARY KEY,
                 Org_name TEXT NOT NULL,
                 API_KEY TEXT NOT NULL)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS agency_table
                 (Org_id INTEGER,
                 Agency_Id INTEGER PRIMARY KEY AUTOINCREMENT,
                 Agency_name TEXT NOT NULL,
                 Agency_email TEXT NOT NULL,
                 FOREIGN KEY (Org_id) REFERENCES organization_table(Org_id))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS user_table
                 (Org_id INTEGER,
                 Emp_Id INTEGER PRIMARY KEY AUTOINCREMENT,
                 Access_level TEXT NOT NULL,
                 User_name TEXT NOT NULL UNIQUE,
                 Password TEXT NOT NULL,
                 IsAdmin TEXT NOT NULL,
                 Application TEXT NOT NULL,
                 FOREIGN KEY (Org_id) REFERENCES organization_table(Org_id))''')
    conn.commit()
 
@app.route('/insert_organization', methods=['POST'])
def insert_organization():
    Org_name = request.form.get('Org_name')
    API_KEY = request.form.get('API_KEY')
    Org_id = request.form.get('Org_id')
    try:
        with get_db_connection() as conn:
            conn.execute("INSERT INTO organization_table (Org_id, Org_name, API_KEY) VALUES (?, ?, ?)", (Org_id, Org_name, API_KEY))
            conn.commit()
        return jsonify({"message": "Inserted into organization_table"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
@app.route('/insert_agency', methods=['POST'])
def insert_agency():
    Org_id = request.form.get('Org_id')
    Agency_name = request.form.get('Agency_name')
    Agency_email = request.form.get('Agency_email')
    try:
        with get_db_connection() as conn:
            conn.execute("INSERT INTO agency_table (Org_id, Agency_name, Agency_email) VALUES (?, ?, ?)", (Org_id, Agency_name, Agency_email))
            conn.commit()
        return jsonify({"message": "Inserted into agency_table"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
@app.route('/insert_user', methods=['POST'])
def insert_user():
    Org_id = request.form.get('Org_id')
    Access_level = request.form.get('Access_level')
    User_name = request.form.get('User_name')
    Password = request.form.get('Password')
    IsAdmin = request.form.get('IsAdmin')
    Application = request.form.get('Application')
    print(Org_id)
    print(Access_level)
    print(User_name)
    print(Password)
    print(Application)
    try:
        with get_db_connection() as conn:
            conn.execute("INSERT INTO user_table (Org_id, Access_level, User_name, Password, IsAdmin, Application) VALUES (?, ?, ?, ?, ?, ?)", (Org_id, Access_level, User_name, Password, IsAdmin, Application))
            conn.commit()
        return jsonify({"message": "Inserted into user_table"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
@app.route('/login', methods=['POST'])
def select_user_by_credentials():
    username = request.form.get('username')
    password = request.form.get('password')
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM user_table WHERE User_name = ? AND Password = ?", (username, password))
        rows = cursor.fetchall()
        conn.close()
        users = []
        for row in rows:
            user = dict(row)
            users.append(user)
        if users:
            return jsonify({"users": users}), 200
        else:
            return jsonify({"message": "No user found with provided credentials"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#############################################################################################################
# Define the upload folder
 
@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('search.html')

@app.route('/recommend',methods=['GET','POST'])
def recommend_id():
    pdf_p = 'static/jd_skills'
    reccom=[f.split('.')[0].split('_')[-1] for f in os.listdir(pdf_p) if f.startswith('Top_Skills_Of_JD')]
    print(reccom)
    #return reccom
    return make_response({"data": reccom, "success": "ok", "error": None})

############################### new 
@app.route('/upload', methods=['POST', 'GET'])
def upload_file():
    try:
        
        if request.method == 'POST':
            # if file not present

            if 'file' not in request.files:
                requisition_id = request.form['rec_id']
                print(requisition_id)
                if requisition_id:
                    print(requisition_id)
                    # Process GET request with requisition_id
                    rec_data = recommend.main(f'./static/jd_skills/Top_Skills_Of_JD_{requisition_id}.xlsx', requisition_id)
                    return make_response({"data": rec_data, "success": "ok", "error": None})
                else:
                    # Handle case when requisition_id is not provided in the GET request
                     return jsonify({"data": None, "success": None, "error": '400'})

            
            file = request.files['file']
            print(file)
            requisition_id = request.form['rec_id']
            print(requisition_id)
            if requisition_id and file:
                print(requisition_id)
                print('true')

                # If user does not select file, browser also
                # submit an empty part without filename
 
                # Check if the file format is allowed
                if file and f.allowed_file(file.filename):
                    filename = file.filename
                    file.save(f"./static/pdf/{filename}")
                    content = parse.get_text(f"./static/pdf/{filename}")
                    response = parse.get_response(parse.System_Prompt, content)
                    data = parse.response_to_df(response, f"./static/pdf/{filename}")
                    data.to_html(header="true", table_id="table")
                    data.to_excel(f'./static/jd_skills/Top_Skills_Of_JD_{requisition_id}.xlsx', index=False)
                    rec_data = recommend.main(f'./static/jd_skills/Top_Skills_Of_JD_{requisition_id}.xlsx', requisition_id)
                    return make_response({"data": rec_data, "success": "ok", "error": None})
            else:
                pass
               
    except Exception as e:
        print(e)
        return jsonify({"data": str(e), "success": None, "error": '500'}), 500  
###############################
@app.route('/search2', methods=['GET', 'POST'])
def search2():
    return render_template('search2.html')


# #### single resume edit 
@app.route('/upload_resume', methods=['POST',"GET"])
def upload_resume_file():
        if request.method == 'POST':
            if 'file' not in request.files:        
                print('No file part')
            files = request.files.getlist('file')    
            for file in files:        
                # Save the file to the desktop or any other desired location
                file.save(f'./static/Data_Resumes_PDF/{file.filename}')   
             
            gen_matrix.start()         
            return 'File(s) uploaded successfully'
        
if __name__ == '__main__':
    app.run(host = "0.0.0.0",port = '5000', debug=True)