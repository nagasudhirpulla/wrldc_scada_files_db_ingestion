from typing import TypedDict


class IJsonConf(TypedDict):
    idSrvDiscoUrl: str
    accessTokenValidationAudience: str
    serverSecret: str
