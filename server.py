from flask import Flask
from src.nodeStatusService.decorators import validate_bearer_jwt
app = Flask(__name__)


@app.route('/')
def index():
    return 'nodes ping status management service'

@app.route('/createNodesPingStatus', methods=['POST'])
@validate_bearer_jwt
def createNodesPingStatus():
    return 'success'


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
