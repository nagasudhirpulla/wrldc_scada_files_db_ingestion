from flask import Flask, request
from src.nodeStatusService.decorators import validate_bearer_jwt
app = Flask(__name__)


@app.route('/')
def index():
    return 'nodes ping status management service'


@app.route('/createNodesPingStatus', methods=['POST'])
@validate_bearer_jwt
def createNodesPingStatus():
    # https://stackoverflow.com/a/23889195
    # send post request with requests module - https://stackoverflow.com/a/35535240
    reqData = request.get_json()
    statusList = reqData['statusList']
    # print(statusList)
    return 'success'


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
