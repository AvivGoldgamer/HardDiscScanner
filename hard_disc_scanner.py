from unittest import result
from venv import create
import config as cfg
import os, stat, hashlib, sqlite3, pandas
from datetime import datetime as dt
import concurrent.futures as cf

################################# Logger Method ##############################

def initialize_loger_collection():

    try:
        
        # Create Collection "Logs"
        conn.execute('''CREATE TABLE LOGS
                    (ID INTEGER PRIMARY KEY AUTOINCREMENT, LOG_DESCRIPTION TEXT, Date TEXT, CUSTOMER_ID TEXT);''')

    except sqlite3.OperationalError:
        
        # In Case Collection Already Exists 
        pass

def create_log(desc):
    
    # Insert Log To DB
    conn.execute(r'''INSERT INTO LOGS (LOG_DESCRIPTION, DATE, CUSTOMER_ID)
                    VALUES ('{}', '{}', '{}');'''.format(desc, dt.now().strftime(r'%d/%m/%Y - %H:%M:%S'), cfg.customer_account_id))
    
    # Save The Log
    conn.commit()

################################# DB Methods #################################

# Connect To Database
conn = sqlite3.connect(cfg.db_name, check_same_thread = False)

# Create Collection For Scans Output
def inititalize_files_collection():
    
    try:
        
        # Create Collection
        conn.execute('''CREATE TABLE FILES
                    (ID INTEGER PRIMARY KEY AUTOINCREMENT, FILE_NAME TEXT, IS_HIDDEN INTEGER, CREATION_DATE TEXT, SHA256 TEXT);''')

    except sqlite3.OperationalError:

        # In Case Collection Already Exists
        pass

# Add File Data To The Collection
def add_file_to_db(file_data):

        # Create Query For File Data Insertion To The DB
    insert_query = r'''INSERT INTO FILES (FILE_NAME, IS_HIDDEN, CREATION_DATE ,SHA256)
                    VALUES ('{}', {}, '{}', '{}');'''.format(
                    file_data["file_name"].replace("'", ""), file_data["is_hidden"], file_data["creation_date"], file_data["sha256"])        

    # Execute The Insert
    conn.execute(insert_query)

################################# Sub Methods #######################################

# Read The File And Hash It
def read_and_hash_file(file_path):
    
    try:

        # Read The File In Binary Mode
        with open(file_path, "rb") as file_temp:
            
            # Read The File Data
            bytes = file_temp.read()

            # Return The Hash Code
            return hashlib.sha256(bytes).hexdigest()

    except PermissionError:
        
        return "ACCESS_TO_FILE_DENIED"

# Extract all the file properties(CTime, Hidden, sha256)
def extract_file_properties(file_path, dir, file_data):
    
    try:

        # Assign File Name
        file_data["file_name"] = dir

        # Get Attribiutes From File And Assign It
        file_data["creation_date"] = dt.fromtimestamp(os.path.getctime(file_path)).strftime(r'%d/%m/%Y - %H:%M:%S')

        # Get Stat From File And Assign It
        file_data["is_hidden"] = bool(os.stat(file_path).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN)

        # Read The File And Create A Hash
        file_data["sha256"] = read_and_hash_file(file_path)

        return file_data

    except (TypeError, FileNotFoundError, OSError):

        # Ignore Above Exceptions
        print("asdasd")

# Check If File Extension Is Same As Config File Extension
def extension_check(file_name):

    # Split File Name
    file_ext_temp = file_name.rsplit(".", 1)


    # If Length Of Array Is below 2 - Didn't Split
    if(len(file_ext_temp) < 2):

        # Checking If The File Extension Filter Is All
        if(cfg.file_extension.lower() == "all"):

            return True
        
        return False

    # Assign The File Extension
    file_ext = file_ext_temp[1].lower()

    # Check File Extension Filter On All Or On
    if(file_ext == cfg.file_extension.lower() or cfg.file_extension.lower() == "all"):
        return True
    
    return False

# Initiate The File Data Retrieval
def get_file_data(dir):

    # Creating Dictionary For File Data
    file_data = {"file_name":"", "is_hidden" : "", "creation_date" : "", "sha256" : ""}

    # Iterating Over All The Files Take From This Current Folder - dir[2] = All Files In Current Folder
    for file_name in dir[2]:

        # Checking If The Extension Filter Is Specific Or All
        if(extension_check(file_name)):
            # Extracting File Properties
            file_data = extract_file_properties(dir[0] + "\\" + file_name, file_name, file_data)

            # Inserting File Data To The
            add_file_to_db(file_data)

################################# Main Methods ######################################

# Initializes The DB
def initialize_db():

    # Initiate Files Collection
    inititalize_files_collection()

    # Initiate Loger Collection
    initialize_loger_collection()

# Reads All The DB Entries That Indicated As Hidden And Write Them To Excel
def result_writer():

    # Selecting All Rows From DB That IS_HIDDEN is True
    data = conn.execute('''SELECT FILE_NAME, CREATION_DATE 
                            FROM FILES 
                            WHERE IS_HIDDEN = 1''')

    # Creating A New Array For Rows To Output
    rows = []

    # Running Over The Data Recieved From The DB
    for value in data:

        print(value)
        
        # Getting The File Name And Extension Seperated
        file_name_splited = value[0].rsplit(".", 1)

        # Checking If File Been Splitted
        if len(file_name_splited) > 1:
            
            # Creating Data Row With All Data Required
            row = [file_name_splited[0], file_name_splited[1], value[1]]

        # Not Splitted
        else:

            # Creating Row With No Extension
            row = [file_name_splited, "", value[1]]

        # Appending Rows To The Array
        rows.append(row)

    # Creating An Empty Data Frame To Store All Rows
    df = pandas.DataFrame(rows, columns=["File Name", "File Extension", "Creation Date"])

    # Converting Data Frame Into Excel
    df.to_excel("Result.xlsx", index = False)

# Initiate Threads And Start Analysis Sequence
def search_for_files(drive_path):

    # Getting All The Sub Folder's Data
    drive_data = os.walk(drive_path, True)
    
    # Initiating Thread Pool Executer To Manage The Threads
    with cf.ThreadPoolExecutor() as executer:

        # Executing get_file_data On Every Sub Folder
        f = executer.map(get_file_data, drive_data)

################################# Main Function #####################################

# Main Function
def scan_hard_disc():

    # Create The Collections "Files" and "Logs"
    initialize_db()
    
    # Create Log For Scanning Start
    create_log("Scanning Started")


    print(dt.now())

    # Run Over All The Drives Chosen
    for drive_path in cfg.hd_letters:
        search_for_files(drive_path + ":\\")

    print(dt.now())

    # Save Inserts To The DB
    conn.commit()    

    # Create The Result Excel
    result_writer()

    # Create Log For Scanning Complete
    create_log("Scanning Complete")


################################# Main Function Call ################################

scan_hard_disc()
