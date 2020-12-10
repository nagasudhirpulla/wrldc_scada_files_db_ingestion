from flask import Flask, request
from src.nodeStatusService.decorators import validate_bearer_jwt
from src.appConf import getJsonConfig
from src.statusFilesHandler import StatusFilesHandler
import werkzeug

app = Flask(__name__)

jsonConf = getJsonConfig()
idSrvDiscoUrl = jsonConf['idSrvDiscoUrl']
accessTokenValidationAudience = jsonConf['accessTokenValidationAudience']


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
    if set('ip,status,name,data_time'.split(',')).issubset(statusList[0].keys()):
        raise werkzeug.exceptions.InternalServerError()

    handler = StatusFilesHandler()
    handler.pushDataRowsToDb(statusList)
    # print(statusList)
    return 'success'


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
