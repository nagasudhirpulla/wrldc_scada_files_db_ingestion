import werkzeug
from functools import wraps
from flask import request
from src.nodeStatusService.idSrvUtils import validateJwt


# def validate_bearer_jwt(f):
#     @wraps(f)
#     def decorated_view(*args, **kwargs):
#         idSrvDiscoEndPnt = ""
#         audience = ""
#         try:
#             encodedJwt: str = request.headers['Authorization']
#             if not encodedJwt.lower().startswith("bearer "):
#                 raise werkzeug.exceptions.Forbidden()
#             encodedJwt = encodedJwt[len("bearer "):]
#             decoded = validateJwt(encodedJwt, idSrvDiscoEndPnt, audience)
#         except Exception as err:
#             raise werkzeug.exceptions.Forbidden()
#         return f(*args, **kwargs)
#     return decorated_view


def validate_bearer_jwt(discoUrl: str, validationAudience: str):
    def _validate_bearer_jwt(f):
        @wraps(f)
        def decorated_view(*args, **kwargs):
            try:
                encodedJwt: str = request.headers['Authorization']
                if not encodedJwt.lower().startswith("bearer "):
                    raise werkzeug.exceptions.Forbidden()
                encodedJwt = encodedJwt[len("bearer "):]
                decoded = validateJwt(encodedJwt, discoUrl, validationAudience)
            except Exception as err:
                raise werkzeug.exceptions.Forbidden()
            return f(*args, **kwargs)
        return decorated_view
    return _validate_bearer_jwt
