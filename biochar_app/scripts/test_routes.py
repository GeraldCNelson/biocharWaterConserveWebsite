import pytest
from biochar_app.app import app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_home_route(client):
    response = client.get('/')
    print(response.get_data(as_text=True))
    assert response.status_code == 200
    assert b"<title>Biochar Project</title>" in response.data

def test_plot_raw_route(client):
    payload = {
        'year': 2024,
        'variable': 'VWC',
        'strip': 'S1',
        'depth': '1',
        'start_date': '2024-01-01',
        'end_date': '2024-12-31'
    }
    response = client.post('/plot_raw', json=payload)
    print("/plot_raw response:", response.status_code, response.get_data(as_text=True))
    assert response.status_code == 200

def test_plot_raw_route_bad_input(client):
    payload = {
        'year': 'not_a_year',  # invalid year
        'variable': 'INVALID',  # invalid variable
    }
    response = client.post('/plot_raw', json=payload)
    print("/plot_raw bad input response:", response.status_code, response.get_data(as_text=True))
    assert response.status_code in (400, 422, 500)

def test_plot_ratio_route(client):
    payload = {
        'year': 2024,
        'variable': 'VWC',
        'depth': '1',
        'start_date': '2024-01-01',
        'end_date': '2024-12-31'
    }
    response = client.post('/plot_ratio', json=payload)
    print("/plot_ratio response:", response.status_code, response.get_data(as_text=True))
    assert response.status_code == 200

def test_plot_ratio_route_bad_input(client):
    payload = {
        'year': 'bad',
        'variable': 'UNKNOWN'
    }
    response = client.post('/plot_ratio', json=payload)
    print("/plot_ratio bad input response:", response.status_code, response.get_data(as_text=True))
    assert response.status_code in (400, 422, 500)
