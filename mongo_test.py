from pymongo import MongoClient
import gridfs
import certifi
from test_info import UserData


def mongo_conn(username, password):
    """IHS Cluster0 Database Connector
    
    Attempts to connect to the Cluster0 MongoDB database using a given username and password embedded within a URI string.

    Note: usernames can only be authorized by the database administrator.
    If you find that you run into authentication issues when trying to connect to
    the IHS database, feel free to send me an email. 

    A connection to the Cluster0 database is initiated within the variable "conn". 
    The second parameter of the MongoClient() instance, "tlsCAFile", is a reference
    to the location of the certificate authority file used in authentication for the 
    TLS encryption protocol leveraged in establishing the connection. The use of the
    'where()' function within the certifi module returns the location of the 
    certificate authority bundle within Python. 

    If the user trying to connect to the cluster has been authorized, the function returns a handle to 
    an instance of the grid_fs class that can be leveraged within an instance of the Cluster0 database returned
    as "conn".

    Parameters
    ----------
    username : str
        The username respective to your MongoDB account.
    
    password : str
        The password respective to your MongoDB account. 
        
        If you intend on pushing any code which references your password, 
        be sure to mask it before doing so. 
        
        To accomplish this, you can import a variable which contains 
        your password from another file (see line 4), 
        then include the file from which you imported the variable (in this case, 
        'test_info') in a .gitignore file. This will ensure that this file, which
        contains sensitive information, will never be pushed to any repository 
        of yours.   



    Raises
    ------
    Exception
        In the event of any complications arising which may prevent you from 
        connecting to the cluster, and exception will be raised. A specific 
        error message will then be returned. 
    """
    try:
        conn = MongoClient(f"mongodb+srv://{username}:{password}@cluster0.ikpnj7p.mongodb.net/?retryWrites=true&w=majority", tlsCAFile=certifi.where())
        return conn.grid_file
    except Exception as e:
        print('Error in attempting to connect:', e)

# In the below line, I import my password with the use of a method respective to an object imported
# from a separate file. 

# This file is included in a .gitignore, and will not be pushed to the repository.

# I then store my password in the user_password variable. 

user_password = UserData().ret_pass()

# After this, I use the mongo_conn() function to connect to the Cluster0 database,
# then return an instance of the grid_file handle within the MongoClient module. 

# Leveraging the mongo_conn() function will allow us to use GridFS in the
# addition and retrieval of information within the Cluster0 database.

db = mongo_conn('dgetzen', user_password)

# The below line initializes the grid_file handle stored within db, allowing
# access to various methods within the GridFS and grid_file modules.
fs = gridfs.GridFS(db)

# In the line below, the name of the file to upload, or put, onto the database. 
# Ordinarily, this would be a file stored on your local device that you'd want 
# to upload to the database with GridFS.
name_to_put = 'PeopleGroupAreas.geojson'

# In the line below, the absolute location of the directory containing the file to upload. 
location_of_file = 'C:/Users/Owner/Desktop/mongodb-database-tools-windows-x86_64-100.6.1/bin/'

def put_file(file_location, file_name):
    """File Uploader Using GridFS
    
    Uses the instance of the grid_file handle returned
    by the mongo_conn function to upload, or put, a file onto the Cluster0 database.  

    Parameters
    ----------
    file_location : str
        The absolute location of the directory containing the file to upload.
    
    file_name : str
        The name of the file to upload, or put, onto the database.    

    Raises
    ------
    FileNotFoundError
        In the event that either the name of the file or its absolute location
        is erroneous, a FileNotFoundError will be raised. 
    """
    try:
        file_data = open(file_location, "rb")
        data = file_data.read()
        fs.put(data, filename = file_name)
        print("Upload Finished")
    except FileNotFoundError:
        print("Invalid file name or path.")

put_file(name_to_put, location_of_file)

def read_file(file_name):
    """File Reader Using GridFS
    
    Uses the instance of the grid_file handle returned
    by the mongo_conn function to read files from the Cluster0 database.  

    Returns the data of the file read-in. 

    Parameters
    ----------
    
    file_name : str
        The name of the file within the Cluster0 database to read from.    
    """
    data = db.fs.files.find_one({'filename': file_name})
    my_id = data['_id']
    outputdata = fs.get(my_id).read()
    return outputdata

geojson_data = read_file(name_to_put)




