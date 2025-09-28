import requests


def make_post_request(url, data):
    response = requests.post(url, data=data)
    return response.json()


def GetBroveikUserProfile(base_url, token, u_hash):
    data = {"token": token, "u_hash": u_hash}
    data = make_post_request(base_url + "user", data)
    return data
def GetBroveikUserTeam(base_url, token, u_hash):
    data = {"token": token, "u_hash": u_hash}
    data = make_post_request(base_url + "user/authorized/car", data)
    return data