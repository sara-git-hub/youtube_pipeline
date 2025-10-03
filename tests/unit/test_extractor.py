import pytest
import tempfile
import os
import json
from types import SimpleNamespace
from include.youtube.extractor import YouTubeExtractor
from unittest.mock import patch, MagicMock


# ---------------------------
# Fixtures
# ---------------------------
@pytest.fixture
def extractor():
    # Création d'une instance sans appeler __init__
    obj = YouTubeExtractor.__new__(YouTubeExtractor)
    # On simule les attributs utilisés dans les méthodes
    obj.channel_handle = "@MrBeast"
    return obj


# ---------------------------
# Tests des utilitaires de durée
# ---------------------------
def test_iso8601_to_seconds(extractor):
    assert extractor._iso8601_to_seconds("PT1H2M3S") == 3723
    assert extractor._iso8601_to_seconds("PT15M") == 900
    assert extractor._iso8601_to_seconds("PT45S") == 45
    assert extractor._iso8601_to_seconds(None) == 0
    assert extractor._iso8601_to_seconds("INVALID") == 0


def test_seconds_to_readable(extractor):
    assert extractor._seconds_to_readable(3723) == "1:02:03"
    assert extractor._seconds_to_readable(900) == "15:00"
    assert extractor._seconds_to_readable(45) == "0:45"
    assert extractor._seconds_to_readable(0) == "0:00"


# ---------------------------
# Test du parse_video_data
# ---------------------------
def test_parse_video_data(extractor):
    video_item = {
        "id": "abc123",
        "snippet": {
            "title": "Test Video",
            "publishedAt": "2025-10-03T10:00:00Z"
        },
        "statistics": {
            "viewCount": "100",
            "likeCount": "10",
            "commentCount": "5"
        },
        "contentDetails": {
            "duration": "PT1M30S"
        }
    }

    result = extractor._parse_video_data(video_item)
    assert result["video_id"] == "abc123"
    assert result["title"] == "Test Video"
    assert result["view_count"] == "100"
    assert result["like_count"] == "10"
    assert result["comment_count"] == "5"
    assert result["duration"] == "PT1M30S"
    assert result["duration_readable"] == "1:30"


# ---------------------------
# Test save_to_json
# ---------------------------
def test_save_to_json(extractor):
    data = [{"video_id": "abc123", "title": "Test"}]

    # channel_handle simulé
    extractor.channel_handle = "@MrBeast"

    with tempfile.TemporaryDirectory() as tmpdir:
        filename = extractor.save_to_json(data, tmpdir)
        assert os.path.exists(filename)

        with open(filename, "r", encoding="utf-8") as f:
            payload = json.load(f)
        assert payload["total_videos"] == 1
        assert payload["videos"] == data
        assert payload["channel_handle"] == "@MrBeast"

# ---------------------------
# Test get_channel_videos avec mock API
# ---------------------------
@patch("include.youtube.extractor.build")
def test_get_channel_videos(mock_build, extractor):
    # Mock de l'objet youtube
    mock_youtube = MagicMock()
    mock_build.return_value = mock_youtube
    extractor.youtube = mock_youtube  # <- Ajouter ceci

    # Mock _get_uploads_playlist_id()
    extractor._get_uploads_playlist_id = MagicMock(return_value="UPLOADS_ID")

    # Simuler playlistItems().list().execute()
    playlist_response = {
        "items": [{"snippet": {"resourceId": {"videoId": "vid1"}}}],
        "nextPageToken": None
    }
    playlist_mock = MagicMock()
    playlist_mock.list.return_value.execute.return_value = playlist_response
    mock_youtube.playlistItems.return_value = playlist_mock

    # Simuler videos().list().execute()
    videos_response = {
        "items": [
            {
                "id": "vid1",
                "snippet": {"title": "Video 1", "publishedAt": "2025-10-03T10:00:00Z"},
                "statistics": {"viewCount": "100", "likeCount": "10", "commentCount": "5"},
                "contentDetails": {"duration": "PT1M"}
            }
        ]
    }
    videos_mock = MagicMock()
    videos_mock.list.return_value.execute.return_value = videos_response
    mock_youtube.videos.return_value = videos_mock

    # Appel de la méthode
    result = extractor.get_channel_videos(max_results=1)

    # Assertions
    assert len(result) == 1
    assert result[0]["video_id"] == "vid1"
    assert result[0]["title"] == "Video 1"
    assert result[0]["view_count"] == "100"
    assert result[0]["like_count"] == "10"
    assert result[0]["comment_count"] == "5"
    assert result[0]["duration_readable"] == "1:00"