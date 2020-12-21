from flask import Flask, request
from src.nodeStatusService.decorators import validate_bearer_jwt
from src.appConf import getJsonConfig
from src.statusFilesHandler import StatusFilesHandler
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.exceptions import NotFound, InternalServerError
from typing import Any, cast
from waitress import serve

app = Flask(__name__)

jsonConf = getJsonConfig()
idSrvDiscoUrl = jsonConf['idSrvDiscoUrl']
accessTokenValidationAudience = jsonConf['accessTokenValidationAudience']

appPrefix = jsonConf['appPrefix']
serverMode = jsonConf['serverMode']
serverHost = jsonConf['serverHost']
serverPort = jsonConf['serverPort']

app.config['SECRET_KEY'] = jsonConf['serverSecret']


@app.route('/')
def index():
    return 'nodes ping status management service'


@app.route('/createNodesPingStatus', methods=['POST'])
@validate_bearer_jwt(idSrvDiscoUrl, accessTokenValidationAudience)
def createNodesPingStatus():
    # https://stackoverflow.com/a/23889195
    # send post request with requests module - https://stackoverflow.com/a/35535240
    # get request json
    reqData = request.get_json()
    # get status rows from json
    statusList = reqData['statusList']

    # check the structure of rows in the status list
    if len(statusList) == 0:
        return {'message': 'success'}
    if not set('ip,status,name,data_time'.split(',')).issubset(set(statusList[0].keys())):
        raise InternalServerError(description="All required keys not present in the first payload object")

    handler = StatusFilesHandler()
    handler.pushDataRowsToDb(statusList)
    # print(statusList)
    return 'success'


hostedApp = Flask(__name__)
hostedApp.config['SECRET_KEY'] = jsonConf['serverSecret']

cast(Any, hostedApp).wsgi_app = DispatcherMiddleware(NotFound(), {
    appPrefix: app
})

if __name__ == '__main__':
    if serverMode.lower() == 'd':
        hostedApp.run(host=serverHost, port=int(
            serverPort), debug=True)
    else:
        serve(app, host=serverHost, port=int(
            serverPort), url_prefix=appPrefix, threads=1)
