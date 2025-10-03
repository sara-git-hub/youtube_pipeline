import json
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from airflow.models import Variable


class YouTubeExtractor:
    """Extracteur de données YouTube basé sur le handle et la playlist 'uploads'."""

    def __init__(self):
        self.api_key = Variable.get("youtube_api_key")
        self.youtube = build("youtube", "v3", developerKey=self.api_key)

        # Handle par défaut = @MrBeast
        self.channel_handle = Variable.get("youtube_channel_handle", "@MrBeast")
        self.channel_id = self._resolve_channel_id(self.channel_handle)

    def _resolve_channel_id(self, handle: str) -> str:
        """Résout l'ID de chaîne à partir d'un handle (@MrBeast)"""
        try:
            # Supprimer le @ si présent
            handle = handle.lstrip("@")
            response = self.youtube.channels().list(
                part="id,snippet",
                forHandle=handle
            ).execute()

            if not response["items"]:
                raise ValueError(f"Impossible de trouver la chaîne pour handle: {handle}")

            return response["items"][0]["id"]
        except HttpError as e:
            logging.error(f"Erreur API YouTube (résolution handle): {e}")
            raise

    def _get_uploads_playlist_id(self) -> str:
        """Récupère l'ID de la playlist 'uploads' d'une chaîne"""
        response = self.youtube.channels().list(
            part="contentDetails",
            id=self.channel_id,
        ).execute()

        if not response["items"]:
            raise ValueError("Impossible de récupérer la playlist 'uploads'")

        uploads_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        return uploads_id

    def get_channel_videos(self, max_results: int = 200) -> List[Dict]:
        """
        Récupère toutes les vidéos d'une chaîne via sa playlist 'uploads'
        max_results = limite totale de vidéos à extraire
        """
        try:
            uploads_id = self._get_uploads_playlist_id()
            videos_data = []
            next_page_token = None

            while len(videos_data) < max_results:
                playlist_request = self.youtube.playlistItems().list(
                    playlistId=uploads_id,
                    part="snippet",
                    maxResults=50,
                    pageToken=next_page_token,
                )
                playlist_response = playlist_request.execute()

                video_ids = [
                    item["snippet"]["resourceId"]["videoId"]
                    for item in playlist_response["items"]
                ]
                if not video_ids:
                    break

                videos_request = self.youtube.videos().list(
                    id=",".join(video_ids),
                    part="snippet,statistics,contentDetails",
                )
                videos_response = videos_request.execute()

                for item in videos_response["items"]:
                    video_data = self._parse_video_data(item)
                    videos_data.append(video_data)

                next_page_token = playlist_response.get("nextPageToken")
                if not next_page_token:
                    break

            logging.info(
                f"Extraites {len(videos_data)} vidéos via playlist. "
                f"Quota estimé: ~{(len(videos_data) // 50) * 2} unités"
            )
            return videos_data[:max_results]

        except HttpError as e:
            logging.error(f"Erreur API YouTube: {e}")
            raise
        except Exception as e:
            logging.error(f"Erreur extraction: {e}")
            raise

    def _parse_video_data(self, item: Dict) -> Dict:
        """Parse et transforme les données d'une vidéo"""
        snippet = item["snippet"]
        statistics = item.get("statistics", {})
        content_details = item.get("contentDetails", {})

        duration_iso = content_details.get("duration")
        duration_seconds = self._iso8601_to_seconds(duration_iso)
        duration_readable = self._seconds_to_readable(duration_seconds)

        return {
            "title": snippet.get("title", ""),
            "duration": duration_iso,
            "video_id": item["id"],
            "like_count": str(statistics.get("likeCount", "0")),
            "view_count": str(statistics.get("viewCount", "0")),
            "published_at": snippet.get("publishedAt"),
            "comment_count": str(statistics.get("commentCount", "0")),
            "duration_readable": duration_readable,
        }

    def save_to_json(self, data: List[Dict], filepath: str) -> str:
        """Sauvegarde les données au format demandé"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filepath}/youtube_videos_{timestamp}.json"

        payload = {
            "channel_handle": self.channel_handle,
            "extraction_date": datetime.now().isoformat(),
            "total_videos": len(data),
            "videos": data,
        }

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

        logging.info(f"Données sauvegardées: {filename}")
        return filename

    # ---------------------------
    # Utilitaires pour la durée
    # ---------------------------
    def _iso8601_to_seconds(self, duration: Optional[str]) -> int:
        """Convertit 'PT1H2M3S' -> secondes"""
        if not duration:
            return 0
        m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
        if not m:
            return 0
        hours = int(m.group(1) or 0)
        minutes = int(m.group(2) or 0)
        seconds = int(m.group(3) or 0)
        return hours * 3600 + minutes * 60 + seconds

    def _seconds_to_readable(self, secs: int) -> str:
        """Formate secondes -> H:MM:SS ou M:SS"""
        h, rem = divmod(secs, 3600)
        m, s = divmod(rem, 60)
        if h:
            return f"{h}:{m:02d}:{s:02d}"
        return f"{m}:{s:02d}"
