import werkzeug
from functools import wraps
from flask import request
from src.nodeStatusService.idSrvUtils import validateJwt

def validate_bearer_jwt(f):
    @wraps(f)
    def decorated_view(*args, **kwargs):
        idSrvDiscoEndPnt = "https://portal.wrldc.in/idSts/.well-known/openid-configuration"
        audience = "admin@wrldcIdSrv_api"
        try:
            encodedJwt: str = request.headers['Authorization']
            if not encodedJwt.lower().startswith("bearer "):
                raise werkzeug.exceptions.Forbidden()
            encodedJwt = encodedJwt[len("bearer "):]
            decoded = validateJwt(encodedJwt, idSrvDiscoEndPnt, audience)
        except Exception as err:
            raise werkzeug.exceptions.Forbidden()
        return f(*args, **kwargs)
    return decorated_view
