from flask import Flask, request, render_template, send_file
from clickhouse_driver import Client
import pandas as pd
import os, uuid

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def get_clickhouse_client(host, port, database, user, jwt_token):
    return Client(host=host, port=port, user=user, database=database, secure=True, password=jwt_token)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_csv_to_clickhouse():
    file = request.files['file']
    host = request.form['host']
    port = int(request.form['port'])
    db = request.form['database']
    user = request.form['user']
    token = request.form['jwt']
    table = request.form['table']

    filename = os.path.join(UPLOAD_FOLDER, str(uuid.uuid4()) + "_" + file.filename)
    file.save(filename)
    df = pd.read_csv(filename)

    client = get_clickhouse_client(host, port, db, user, token)
    columns = ', '.join(f"{col} String" for col in df.columns)
    client.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns}) ENGINE = MergeTree() ORDER BY tuple()")
    client.execute(f"INSERT INTO {table} VALUES", df.to_dict('records'))

    return "Data uploaded successfully!"

@app.route('/download', methods=['POST'])
def download_from_clickhouse():
    host = request.form['host']
    port = int(request.form['port'])
    db = request.form['database']
    user = request.form['user']
    token = request.form['jwt']
    table = request.form['table']

    client = get_clickhouse_client(host, port, db, user, token)
    query = f"SELECT * FROM {table}"
    df = pd.DataFrame(client.execute(query), columns=[col[0] for col in client.execute(f"DESCRIBE TABLE {table}")])

    download_file = os.path.join(UPLOAD_FOLDER, f"{table}_export.csv")
    df.to_csv(download_file, index=False)

    return send_file(download_file, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
