import requests
import pytest

#definiendo funcion
def test_get_api():
    response = requests.get(
        'https://jsonplaceholder.typicode.com/users/1'
    )
    assert response.status_code ==200
    assert response.json()['id'] ==1