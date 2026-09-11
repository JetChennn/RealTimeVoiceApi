from fastapi.testclient import TestClient

from realtime_voice.config import Settings
from realtime_voice.main import create_app


def test_test_page_assets_and_traversal_boundary():
    app = create_app(Settings(_env_file=None))
    with TestClient(app) as client:
        response = client.get('/test')
        assert response.status_code == 200
        assert '实时语音测试台' in response.text
        for asset in ('app.mjs', 'core.mjs', 'style.css', 'audio-worklet.js'):
            response = client.get('/test/' + asset)
            assert response.status_code == 200
            if asset.endswith(('.mjs', '.js')):
                assert 'javascript' in response.headers['content-type']
        assert client.get('/test/%2e%2e/config.py').status_code == 404
        assert client.get('/test/nonexistent').status_code == 404
