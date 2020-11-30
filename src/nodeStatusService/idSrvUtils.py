import requests
import jwt
import json


def getIdSrvJwk(discoUrl):
    jwkUrl = requests.get(discoUrl).json()['jwks_uri']
    keys = requests.get(jwkUrl).json()['keys']
    return keys


def getPublicJwKKey(encodedJwt: str, discoUrl):
    pubKeys = getIdSrvJwk(discoUrl)
    jwtKid = jwt.get_unverified_header(encodedJwt)['kid']
    # find the key with required kid
    pubKey = None
    for k in pubKeys:
        if k['kid'] == jwtKid:
            pubKey = k
            break
    return pubKey


def validateJwt(encodedJwt: str, discoUrl: str, audience: str):
    pubJwKKey = getPublicJwKKey(encodedJwt, discoUrl)
    publicKey = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(pubJwKKey))
    decoded = jwt.decode(encodedJwt, publicKey,
                         algorithms='RS256', audience=audience)
    return decoded
