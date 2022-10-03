import pickle
import time
from threading import Thread

import pandas as pd
from flask import Flask, request, jsonify
from flask_pymongo import PyMongo

import prediction_service

rest_port = 8052

app = Flask(__name__)
app.config["MONGO_URI"] = 'mongodb://root:123456@mongo:27018/prediccion?authSource=admin'
# app.config["MONGO_URI"] = 'mongodb://root:123456@mongo:27017/prediccion?authSource=admin'
mongo = PyMongo(app)


@app.route('/prueba', methods=["POST"])
def prueba():
    return "Conexion "


def predict_attack(data):
    data = pd.DataFrame.from_dict(data)
    model_type = data.model[0]
    data.drop(['model'], axis=1, inplace=True)
    # data = pre_processing(data)
    if model_type != 'dt' and model_type != 'lr' and model_type != 'rf' and model_type != 'svm-linear':
        model_type = 'dt'
    file = mongo.db.fs.files.find_one({'filename': model_type})  # model_type can be: dt, lr, rf, svm-linear
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
    response.append(model_type)
    return response


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
