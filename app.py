from flask import Flask, jsonify, request
from flask_cors import CORS
from flaskext.mysql import MySQL
import boto3

app = Flask(__name__)
CORS(app)

# Configure MySQL database
app.config['MYSQL_DATABASE_HOST'] = 'image-uploader-db-363215.cc1mavvnwdua.us-east-1.rds.amazonaws.com'
app.config['MYSQL_DATABASE_USER'] = 'db_admin'
app.config['MYSQL_DATABASE_PASSWORD'] = 'dbadmin363215'
app.config['MYSQL_DATABASE_DB'] = 'mysql'

mysql = MySQL()
mysql.init_app(app)

# s3 = boto3.client('s3', aws_access_key_id='AKIAYLNQSMB6S2HD5RSO',
#                   aws_secret_access_key='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')



# Configure AWS S3 bucket
s3 = boto3.client('s3', aws_access_key_id='AKIA3KRWYUQJ656R5542',
                  aws_secret_access_key='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')
bucket_name = 'image-uploader-363215'




def insert_new_user(username, password):
    return f"INSERT INTO CC.user_credentials (username, password, storage_used) VALUES ('{username}', '{password}', 0)"

def find_user_by_username_and_password(username, password):
    return f"SELECT * FROM CC.user_credentials WHERE username='{username}' AND password='{password}'"

def find_user_by_username(username):
    return f"SELECT * FROM CC.user_credentials WHERE username='{username}'"

def insert_user_data(username, imagePath, imageSize):
    return f"INSERT INTO CC.user_data (log_date, username, image_path, image_size_mb, is_deleted) VALUES (CURRENT_DATE(), '{username}','{imagePath}','{imageSize}', 'N')"

def get_user_data(username):
    return f"SELECT * FROM CC.user_data WHERE username='{username}' and is_deleted='N'"

def update_storage_used(storage_used, username):
    return f"UPDATE CC.user_credentials SET storage_used='{storage_used}' WHERE username='{username}'"

def get_usage_alert(username):
    return f'''
            SELECT 
                SUM(image_size_mb) `usage`
            FROM
                CC.user_data
            WHERE
                log_date = CURRENT_DATE()
                    AND username = '{username}'
           '''

def set_image_delete(username, id):
    return f"UPDATE CC.user_data SET is_deleted = 'Y' WHERE image_id = {id} AND username = '{username}';"

def get_image_by_id(username, id):
    return f"SELECT image_size_mb from CC.user_data where image_id={id} and username='{username}'"


@app.route('/register', methods=['POST'])
def register():
    request_data = request.json
    username = request_data["username"]
    password = request_data["password"]
    
    cursor = mysql.get_db().cursor()
    cursor.execute(find_user_by_username(username))
    result = cursor.fetchone()
    print(result)
    if result:
        cursor.close()
        return jsonify(error='Username already exists!')
    else:
        cursor.execute(insert_new_user(username, password))
        mysql.get_db().commit()
        cursor.close()
        return jsonify(status=200)

@app.route('/login', methods=['POST'])
def login():
    request_data = request.json
    username = request_data["username"]
    password = request_data["password"]
    
    cursor = mysql.get_db().cursor()
    cursor.execute(find_user_by_username_and_password(username,password))
    result = cursor.fetchone()
    cursor.close()

    if result:
       return jsonify(storage_used=result[2])
    else:
        return jsonify(error="Either username or password is incorrect")
    
@app.route('/<username>/image-upload', methods=['POST'])
def upload_image(username):
    print(username)
    cursor = mysql.get_db().cursor()
    try:
        image = request.files['img']
        size = request.form["size"]
        storage_used = request.form["storageUsed"]
        
        filename = f"{username}/{image.filename}"

        s3.upload_fileobj(image, bucket_name, filename)
        signed_url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': filename,
                },
                ExpiresIn=2592000
            )

        cursor.execute(insert_user_data(username, signed_url, size))
        cursor.execute(update_storage_used(storage_used,username))
        mysql.get_db().commit()
        cursor.close()
    except Exception as e:
        print(e)
        return jsonify(error="An error occured")
    finally:
        cursor.close()
    return jsonify(error=None)
    
    

@app.route('/<username>/user-info', methods=['GET'])
def get_user_info(username):
    cursor = mysql.get_db().cursor()
    cursor.execute(get_user_data(username))
    user_info = cursor.fetchall()
    cursor.execute(find_user_by_username(username))
    user_data = cursor.fetchone()
    cursor.execute(get_usage_alert(username))
    usage_alert = cursor.fetchone()
    cursor.close()
    
    return jsonify(storage_usage_per_day=usage_alert[0],storage_used=user_data[2],user_info=user_info)

@app.route('/<username>/images/<id>', methods=['DELETE'])
def delete_image(username, id):

    cursor = mysql.get_db().cursor()

    cursor.execute(get_image_by_id(username, id))
    image_data = cursor.fetchone()
    image_size = image_data[0]
    cursor.execute(find_user_by_username(username))
    user_data = cursor.fetchone()
    storage_used = user_data[2]

    cursor.execute(update_storage_used(storage_used-image_size, username))

    print(set_image_delete(username,id))
    cursor.execute(set_image_delete(username,id))
    mysql.get_db().commit()

    cursor.close()

    return jsonify(status=200)

if __name__ == '__main__':
    app.run(debug=True)





        # try:
        #     s3.head_object(Bucket=bucket_name, Key=(foldername))
        # except botocore.exceptions.ClientError:
        #     # if the folder doesn't exist, create it
        #     s3.put_object(Bucket=bucket_name, Key=(foldername))