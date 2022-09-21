import json
import pickle
import time
from threading import Thread

import pandas as pd
import py_eureka_client.eureka_client as eureka_client
from flask import Flask, request, jsonify
from flask_pymongo import PyMongo

import prediction_service

rest_port = 8061

app = Flask(__name__)
# app.config["MONGO_URI"] = 'mongodb://root:123456@mongo:27018/prediccion?authSource=admin'
app.config["MONGO_URI"] = 'mongodb://root:123456@localhost:27018/prediccion?authSource=admin'
mongo = PyMongo(app)


@app.route('/prueba', methods=["POST"])
def prueba():
    return "Conexion "


def predict_dt(data):
    data = pd.DataFrame.from_dict(data)
    print(data.head())
    # data = pre_processing(data)

    file = mongo.db.fs.files.find_one({'filename': 'dt'})
    print("paso file")
    binary = b""
    s = mongo.db.fs.chunks.find({'files_id': file['_id']})
    for i in s:
        binary += i['data']
    print("paso for")
    model = pickle.loads(binary)
    print("paso model")
    start = time.time()
    predict = model.predict(data)
    end_predict = time.time() - start
    response = predict.tolist()
    response.append(end_predict)
    print(f'response {response}')
    return response


@app.route('/model/lr', methods=["GET"])
def predict_lr(data):
    if data is not None:
        print("here2")
        data = request.data
        print(type(data))
        # type_ml = request.form['type_ml']
        data = data.decode('utf8')
        data = json.loads(data)
        print(f'decode:{data}')
        print(f'tipo{type(data)}')
        data = pd.DataFrame.from_dict(data)
        print(data.head())
        # data = pre_processing(data)

        file = mongo.db.fs.files.find_one({'filename': 'lr'})
        binary = b""
        s = mongo.db.fs.chunks.find({'files_id': file['_id']})
        for i in s:
            binary += i['data']
        model = pickle.loads(binary)
        start = time.time()
        predict = model.predict(data)
        end_predict = time.time() - start
        response = predict.tolist()
        response.append(end_predict)
        return response
    return "no request files", 404


@app.route('/model/rf', methods=["GET"])
def predict_rf():
    if request.method == 'GET':
        print("here")
        if request.files:
            print("here2")
            data = request.files['data'].read()
            print(type(data))
            # type_ml = request.form['type_ml']
            data = data.decode('utf8')
            data = json.loads(data)
            print(f'decode:{data}')
            print(f'tipo{type(data)}')
            data = pd.DataFrame.from_dict(data)
            print(data.head())
            # data = pre_processing(data)

            file = mongo.db.fs.files.find_one({'filename': 'rf'})
            binary = b""
            s = mongo.db.fs.chunks.find({'files_id': file['_id']})
            for i in s:
                binary += i['data']
            model = pickle.loads(binary)
            start = time.time()
            predict = model.predict(data)
            end_predict = time.time() - start
            response = predict.tolist()
            response.append(end_predict)
            return response
        return "no request files"
    return "no method GET"


@app.route('/model/svm-linear', methods=["GET"])
def predict_svm_linear():
    if request.method == 'GET':
        print("here")
        if request.files:
            print("here2")
            data = request.files['data'].read()
            print(type(data))
            # type_ml = request.form['type_ml']
            data = data.decode('utf8')
            data = json.loads(data)
            print(f'decode:{data}')
            print(f'tipo{type(data)}')
            data = pd.DataFrame.from_dict(data)
            print(data.head())
            # data = pre_processing(data)

            file = mongo.db.fs.files.find_one({'filename': 'svm-linear'})
            binary = b""
            s = mongo.db.fs.chunks.find({'files_id': file['_id']})
            for i in s:
                binary += i['data']
            model = pickle.loads(binary)
            start = time.time()
            predict = model.predict(data)
            end_predict = time.time() - start
            response = predict.tolist()
            response.append(end_predict)
            return response
        return "no request files"
    return "no method GET"


@app.route('/save/model/dt', methods=["POST"])
def save_dt():
    if request.method == 'POST':
        if 'model' in request.files:
            model = request.files['model']
            algorithm_files = mongo.db.fs.files
            file = algorithm_files.find_one({'filename': 'dt'})
            if file is not None:
                s = mongo.db.fs.chunks.find({'files_id': file['_id']})
                for i in range(len(list(s))):
                    mongo.db.fs.chunks.delete_one({'files_id': file['_id']})
                algorithm_files.delete_one({'filename': 'dt'})
            mongo.save_file('dt', model)
            response = jsonify({
                'model_name': 'dt',
                'created_time': time.time(),
                'message': 'Update successfully'
            })
            return response, 200
        return not_model
    return not_post


@app.route('/save/model/lr', methods=["POST"])
def save_lr():
    if request.method == 'POST':
        if 'model' in request.files:
            model = request.files['model']
            algorithm_files = mongo.db.fs.files
            file = algorithm_files.find_one({'filename': 'lr'})
            if file is not None:
                s = mongo.db.fs.chunks.find({'files_id': file['_id']})
                for i in range(len(list(s))):
                    mongo.db.fs.chunks.delete_one({'files_id': file['_id']})
                algorithm_files.delete_one({'filename': 'lr'})
            mongo.save_file('lr', model)
            response = jsonify({
                'model_name': 'lr',
                'created_time': time.time(),
                'message': 'Update successfully'
            })
            return response, 200
        return not_model
    return not_post


@app.route('/save/model/rf', methods=["POST"])
def save_rf():
    if request.method == 'POST':
        if 'model' in request.files:
            model = request.files['model']
            algorithm_files = mongo.db.fs.files
            file = algorithm_files.find_one({'filename': 'rf'})
            if file is not None:
                s = mongo.db.fs.chunks.find({'files_id': file['_id']})
                for i in range(len(list(s))):
                    mongo.db.fs.chunks.delete_one({'files_id': file['_id']})
                algorithm_files.delete_one({'filename': 'rf'})
            mongo.save_file('rf', model)
            response = jsonify({
                'model_name': 'rf',
                'created_time': time.time(),
                'message': 'Update successfully'
            })
            return response, 200
        return not_model
    return not_post


@app.route('/save/model/svm-linear', methods=["POST"])
def save_svm_linear():
    if request.method == 'POST':
        if 'model' in request.files:
            model = request.files['model']
            algorithm_files = mongo.db.fs.files
            file = algorithm_files.find_one({'filename': 'svm-linear'})
            if file is not None:
                s = mongo.db.fs.chunks.find({'files_id': file['_id']})
                for i in range(len(list(s))):
                    mongo.db.fs.chunks.delete_one({'files_id': file['_id']})
                algorithm_files.delete_one({'filename': 'svm-linear'})
            mongo.save_file('svm-linear', model)
            response = jsonify({
                'model_name': 'svm-linear',
                'created_time': time.time(),
                'message': 'Update successfully'
            })
            return response, 200
        return not_model
    return not_post


@app.errorhandler(404)
def not_found(error=None):
    response = jsonify({'message': 'Resource Not found:' + request.url, 'status': 404})
    response.status_code = 404
    return response


@app.errorhandler(404)
def not_model(error=None):
    response = jsonify({'message': 'Not model', 'status': 404})
    response.status_code = 404
    return response


@app.errorhandler(404)
def not_post(error=None):
    response = jsonify({'message': 'Is not a POST', 'status': 404})
    response.status_code = 404
    return response


if __name__ == "__main__":
    t = Thread(target=prediction_service.start_service)
    t.start()
    app.run(host='0.0.0.0', port=rest_port)
    # app.run(debug=True, port=rest_port)
