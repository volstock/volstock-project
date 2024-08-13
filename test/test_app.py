from src.app import hello_world

def test_check_hello_world():
    response = hello_world()
    assert response == 'Hello world'