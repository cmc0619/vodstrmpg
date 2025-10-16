import os
import json
import re
import time
import threading
from datetime import datetime
from typing import Dict, Any, List, Tuple, Callable, Set, Optional
import requests
import psycopg2
import psycopg2.extras


# Global scheduler state
_bg_thread = None
_stop_event = threading.Event()

# Global debug logger (thread-safe)
_debug_logger = None
_debug_lock = threading.Lock()


# Constants
DEFAULT_TIMEOUT = 20
API_TIMEOUT = 30
SAFE_NAME_RE = re.compile(r'[\\/:*?"<>|\t\r\n\']+')
DEFAULT_MAX_NAME_LENGTH = 180
TMDB_RATE_LIMIT_DELAY = 0.26
DRY_RUN_LIMIT = 1000

# Database connection constants
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "dispatcharr"
DB_USER = "dispatch"
DB_PASSWORD = "secret"


class DualLogger:
    """Logger wrapper that writes to both Django logger and debug file"""
    def __init__(self, django_logger, debug_logger=None):
        self.django_logger = django_logger
        self.debug_logger = debug_logger
    
    def info(self, msg, *args):
        self.django_logger.info(msg, *args)
        if self.debug_logger:
            self.debug_logger.log(f"INFO: {msg % args if args else msg}")
    
    def warning(self, msg, *args):
        self.django_logger.warning(msg, *args)
        if self.debug_logger:
            self.debug_logger.log(f"WARNING: {msg % args if args else msg}")
    
    def error(self, msg, *args):
        self.django_logger.error(msg, *args)
        if self.debug_logger:
            self.debug_logger.log(f"ERROR: {msg % args if args else msg}")
    
    def exception(self, msg, *args):
        self.django_logger.exception(msg, *args)
        if self.debug_logger:
            self.debug_logger.log(f"EXCEPTION: {msg % args if args else msg}")


def safe_name(s: str, maxlen: int = DEFAULT_MAX_NAME_LENGTH) -> str:
    """Sanitize a string for use as a filename."""
    s = (s or "").strip()
    s = SAFE_NAME_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s)
    s = s[:maxlen].rstrip(" .")
    s = s.lstrip(".")
    return s.strip() or "unknown"


def ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


def read_text(path: str) -> str:
    """Read text file, return empty string on error."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def write_text_if_changed(path: str, content: str, dry_run: bool) -> Tuple[bool, str]:
    """Write text to file only if content differs."""
    existing = read_text(path)
    if existing == content:
        return (False, "unchanged")
    if dry_run:
        return (False, "dry_run")
    tmp = f"{path}.tmp-{int(time.time() * 1000)}"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)
    return (True, "written")


def load_manifest(root: str) -> Dict[str, Any]:
    """Load manifest file or return default."""
    manifest_path = os.path.join(root, ".vod_strm_manifest.json")
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"files": {}, "tmdb_cache": {}, "version": 1}


def save_manifest(root: str, manifest: Dict[str, Any]) -> None:
    """Save manifest file atomically."""
    manifest_path = os.path.join(root, ".vod_strm_manifest.json")
    tmp = f"{manifest_path}.tmp-{int(time.time() * 1000)}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(tmp, manifest_path)


def normalize_host(host: str) -> str:
    """Normalize host by removing scheme and trailing slashes."""
    host = (host or "").strip()
    host = re.sub(r"^\s*https?://", "", host, flags=re.I)
    return host.strip().strip("/")


class Plugin:
    """VOD .STRM Writer Plugin for Dispatcharr proxy."""

    name = "vodstrmpg"
    version = "0.2.0"  # Hybrid version with best of both worlds
    description = "Writes .strm and .nfo files for Movies & Series using Dispatcharr proxy with provider ranking, genre organization, and optimized TMDB batching."

    fields = [
        {"id": "dispatcharr_host", "label": "Dispatcharr Host (host:port)", "type": "string", "default": "tv.local:9191", "help_text": "NO scheme. Example: tv.local:9191"},
        {"id": "tmdb_api_key", "label": "TMDB API Key (optional)", "type": "string", "default": "", "help_text": "Get free API key from themoviedb.org. If empty, uses original names."},
        {"id": "write_nfo_files", "label": "Write NFO Files", "type": "boolean", "default": False, "help_text": "Write .nfo metadata files for Jellyfin/Emby/Plex. Requires TMDB API key."},
        {"id": "organize_by_genre", "label": "Organize by Genre", "type": "boolean", "default": False, "help_text": "Create genre subfolders using TMDB genres (e.g., /movies/Action/). Requires TMDB API key."},
        {"id": "provider_ranking", "label": "Provider Priority (comma-separated IDs)", "type": "string", "default": "", "placeholder": "1,3,2", "help_text": "Comma-separated M3U account IDs in priority order. When duplicates exist (same TMDB ID from multiple providers), the highest ranked provider will be used. Use 'Show Database Statistics' to see provider IDs and names."},
        {"id": "movies_root", "label": "Movies Root", "type": "string", "default": "/VODs/movies"},
        {"id": "series_root", "label": "Series Root", "type": "string", "default": "/VODs/series"},
        {"id": "probe_urls", "label": "Probe URLs (HEAD)", "type": "boolean", "default": False},
        {"id": "cleanup_removed", "label": "Remove Stale .strm", "type": "boolean", "default": False},
        {"id": "dry_run", "label": "Dry Run", "type": "boolean", "default": False, "help_text": "Simulates the run without writing or deleting files. Limits processing to 1000 items. Note: Scheduled runs always run for real."},
        {"id": "verbose", "label": "Verbose Per-Item Logs", "type": "boolean", "default": True},
        {"id": "debug_log", "label": "Debug Logging", "type": "boolean", "default": False, "help_text": "Write detailed debug logs to /data/vod_strm_debug.log (file is overwritten each run)"},
        {"id": "scheduled_times_movies", "label": "Movie Schedule (24-hour format)", "type": "string", "default": "", "placeholder": "0300,1500", "help_text": "Comma-separated times to run movies daily (HHMM format). Example: 0300,1500. Leave blank to disable. Uses system timezone."},
        {"id": "scheduled_times_series", "label": "Series Schedule (24-hour format)", "type": "string", "default": "", "placeholder": "0400,1600", "help_text": "Comma-separated times to run series daily (HHMM format). Example: 0400,1600. Leave blank to disable. Uses system timezone."},
    ]

    actions = [
        {"id": "show_stats", "label": "Show Database Statistics"},
        {"id": "write_movies", "label": "Write Movie .STRM Files"},
        {"id": "write_series", "label": "Write Series .STRM Files"},
    ]

    def __init__(self):
        self.last_tmdb_request = 0
        self.settings_file = "/data/vod_strm_settings.json"
        self.debug_log_file = "/data/vod_strm_debug.log"
        self.debug_logger = None
        # Shared HTTP session with optimizations (from ChatGPT version)
        self.http = self._make_http_session()
        self._load_settings()

    def _make_http_session(self) -> requests.Session:
        """Create optimized HTTP session with retries and proper User-Agent."""
        s = requests.Session()
        s.headers.update({"User-Agent": "dispatcharr-vodstrmpg/0.2.0 (+STRM+NFO+Providers)"})
        try:
            from urllib3.util.retry import Retry
            from requests.adapters import HTTPAdapter
            retry = Retry(
                total=3,
                connect=3,
                read=3,
                backoff_factor=0.3,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET", "HEAD"])
            )
            adapter = HTTPAdapter(max_retries=retry)
            s.mount("http://", adapter)
            s.mount("https://", adapter)
        except Exception:
            # If urllib3 Retry isn't available, proceed without it
            pass
        return s

    def _get_db_connection(self):
        """Get a database connection."""
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

    def _setup_debug_logger(self, enabled):
        """Setup or teardown debug file logger"""
        global _debug_logger
        
        if not enabled:
            with _debug_lock:
                _debug_logger = None
            self.debug_logger = None
            return

        with open(self.debug_log_file, 'w') as f:
            f.write(f"=== VOD STRM Debug Log - {datetime.now().isoformat()} ===\n\n")

        class DebugLogger:
            def __init__(self, filepath):
                self.filepath = filepath

            def log(self, msg):
                try:
                    with _debug_lock:
                        with open(self.filepath, 'a') as f:
                            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
                except Exception:
                    pass

        with _debug_lock:
            _debug_logger = DebugLogger(self.debug_log_file)
        self.debug_logger = _debug_logger

    def _load_settings(self):
        """Load saved settings from disk and start scheduler"""
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r') as f:
                    self.saved_settings = json.load(f)
                    self._start_background_scheduler(self.saved_settings)
            else:
                self.saved_settings = {}
        except Exception:
            self.saved_settings = {}

    def _save_settings(self, settings):
        """Save settings to disk"""
        try:
            with open(self.settings_file, 'w') as f:
                json.dump(settings, f, indent=2)
            self.saved_settings = settings
        except Exception:
            pass

    def _parse_scheduled_times(self, scheduled_times_str):
        """Parse scheduled times string into list of datetime.time objects"""
        if not scheduled_times_str or not scheduled_times_str.strip():
            return []
        times = []
        for time_str in scheduled_times_str.split(','):
            time_str = time_str.strip()
            if len(time_str) == 4 and time_str.isdigit():
                hour = int(time_str[:2])
                minute = int(time_str[2:])
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    times.append(datetime.strptime(time_str, "%H%M").time())
        return times

    def _start_background_scheduler(self, settings):
        """Start background thread for scheduled runs"""
        global _bg_thread

        movies_times_str = settings.get("scheduled_times_movies", "").strip()
        series_times_str = settings.get("scheduled_times_series", "").strip()

        if not movies_times_str and not series_times_str:
            return

        movies_times = self._parse_scheduled_times(movies_times_str)
        series_times = self._parse_scheduled_times(series_times_str)

        if not movies_times and not series_times:
            return

        if _bg_thread and _bg_thread.is_alive():
            _stop_event.set()
            _bg_thread.join(timeout=5)
            _stop_event.clear()

        def scheduler_loop():
            import logging
            from django.utils import timezone as django_tz
            logger = logging.getLogger("vod_strm_scheduler")
            last_run_date = {"movies": None, "series": None}

            while not _stop_event.is_set():
                try:
                    now = django_tz.now()
                    current_date = now.date()

                    for scheduled_time in movies_times:
                        scheduled_dt = django_tz.make_aware(datetime.combine(current_date, scheduled_time))
                        time_diff = (scheduled_dt - now).total_seconds()

                        if -30 <= time_diff <= 30 and last_run_date["movies"] != current_date:
                            logger.info(f"Scheduled movie scan triggered at {now.strftime('%Y-%m-%d %H:%M %Z')}")
                            if _debug_logger:
                                _debug_logger.log(f"=== SCHEDULED MOVIE SCAN TRIGGERED at {now.strftime('%Y-%m-%d %H:%M %Z')} ===")
                            try:
                                scheduled_settings = settings.copy()
                                scheduled_settings["dry_run"] = False

                                result = self._write_movies(scheduled_settings, logger)
                                logger.info(f"Scheduled movie scan completed: {result.get('message', 'Done')}")
                            except Exception as e:
                                logger.error(f"Error in scheduled movie scan: {e}")
                            last_run_date["movies"] = current_date
                            break

                    for scheduled_time in series_times:
                        scheduled_dt = django_tz.make_aware(datetime.combine(current_date, scheduled_time))
                        time_diff = (scheduled_dt - now).total_seconds()

                        if -30 <= time_diff <= 30 and last_run_date["series"] != current_date:
                            logger.info(f"Scheduled series scan triggered at {now.strftime('%Y-%m-%d %H:%M %Z')}")
                            if _debug_logger:
                                _debug_logger.log(f"=== SCHEDULED SERIES SCAN TRIGGERED at {now.strftime('%Y-%m-%d %H:%M %Z')} ===")
                            try:
                                scheduled_settings = settings.copy()
                                scheduled_settings["dry_run"] = False

                                result = self._write_series(scheduled_settings, logger)
                                logger.info(f"Scheduled series scan completed: {result.get('message', 'Done')}")
                            except Exception as e:
                                logger.error(f"Error in scheduled series scan: {e}")
                            last_run_date["series"] = current_date
                            break

                    _stop_event.wait(30)
                except Exception as e:
                    logger.error(f"Error in scheduler loop: {e}")
                    _stop_event.wait(60)

        _bg_thread = threading.Thread(target=scheduler_loop, name="vod-strm-scheduler", daemon=True)
        _bg_thread.start()

    def _stop_background_scheduler(self):
        """Stop background scheduler thread"""
        global _bg_thread
        if _bg_thread and _bg_thread.is_alive():
            _stop_event.set()
            _bg_thread.join(timeout=5)
            _stop_event.clear()

    def _escape_xml(self, text: str) -> str:
        """Escape special XML characters."""
        if not text:
            return ""
        text = str(text)
        text = text.replace("&", "&amp;")
        text = text.replace("<", "&lt;")
        text = text.replace(">", "&gt;")
        text = text.replace('"', "&quot;")
        text = text.replace("'", "&apos;")
        return text

    def _tmdb_lookup_movie(self, tmdb_id: int, api_key: str, logger: Any) -> Optional[Dict[str, Any]]:
        """Lookup movie details from TMDB using optimized HTTP session."""
        try:
            elapsed = time.time() - self.last_tmdb_request
            if elapsed < TMDB_RATE_LIMIT_DELAY:
                time.sleep(TMDB_RATE_LIMIT_DELAY - elapsed)
            url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={api_key}"
            response = self.http.get(url, timeout=DEFAULT_TIMEOUT)
            self.last_tmdb_request = time.time()
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning("TMDB movie lookup failed for ID %s: %s", tmdb_id, response.status_code)
                return None
        except Exception as e:
            logger.warning("TMDB movie lookup error for ID %s: %s", tmdb_id, e)
            return None

    def _tmdb_lookup_series(self, tmdb_id: int, api_key: str, logger: Any) -> Optional[Dict[str, Any]]:
        """Lookup series details from TMDB using optimized HTTP session."""
        try:
            elapsed = time.time() - self.last_tmdb_request
            if elapsed < TMDB_RATE_LIMIT_DELAY:
                time.sleep(TMDB_RATE_LIMIT_DELAY - elapsed)
            url = f"https://api.themoviedb.org/3/tv/{tmdb_id}?api_key={api_key}"
            response = self.http.get(url, timeout=DEFAULT_TIMEOUT)
            self.last_tmdb_request = time.time()
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning("TMDB series lookup failed for ID %s: %s", tmdb_id, response.status_code)
                return None
        except Exception as e:
            logger.warning("TMDB series lookup error for ID %s: %s", tmdb_id, e)
            return None

    def _tmdb_fetch_season_map(self, series_tmdb_id: int, season_num: int, api_key: str, tmdb_cache: Dict[str, Any], logger: Any) -> Optional[Dict[int, Dict[str, Any]]]:
        """
        Fetch TMDB season once, return {episode_number: ep_json}.
        Cached in manifest['tmdb_cache'] with key 'season:{series}:{season}'.
        This is the key optimization from ChatGPT version - batched episode fetching.
        """
        if not (series_tmdb_id and api_key):
            return None

        cache_key = f"season:{series_tmdb_id}:{season_num}"
        cached = tmdb_cache.get(cache_key)
        if cached and isinstance(cached, dict) and "data" in cached:
            data = cached["data"]
            eps = data.get("episodes") or []
            return {int(e.get("episode_number", 0) or 0): e for e in eps}

        try:
            elapsed = time.time() - self.last_tmdb_request
            if elapsed < TMDB_RATE_LIMIT_DELAY:
                time.sleep(TMDB_RATE_LIMIT_DELAY - elapsed)
            url = f"https://api.themoviedb.org/3/tv/{series_tmdb_id}/season/{season_num}?api_key={api_key}"
            r = self.http.get(url, timeout=DEFAULT_TIMEOUT)
            self.last_tmdb_request = time.time()
            if r.status_code != 200:
                logger.warning("[TMDB] season fetch failed: series=%s season=%s status=%s", series_tmdb_id, season_num, r.status_code)
                return None
            season_json = r.json()
            tmdb_cache[cache_key] = {"fetched_at": datetime.utcnow().isoformat() + "Z", "data": season_json}
            eps = season_json.get("episodes") or []
            return {int(e.get("episode_number", 0) or 0): e for e in eps}
        except Exception as e:
            logger.warning("[TMDB] season fetch error: series=%s season=%s err=%s", series_tmdb_id, season_num, e)
            return None

    def _write_movie_nfo(self, filepath: str, tmdb_data: Dict[str, Any], dry_run: bool) -> bool:
        """Write movie NFO file."""
        if dry_run:
            return False
        try:
            nfo_path = filepath.rsplit('.', 1)[0] + '.nfo'
            title = tmdb_data.get("title", "")
            original_title = tmdb_data.get("original_title", "")
            plot = tmdb_data.get("overview", "")
            rating = tmdb_data.get("vote_average", 0)
            year = tmdb_data.get("release_date", "")[:4] if tmdb_data.get("release_date") else ""
            runtime = tmdb_data.get("runtime", 0)
            tmdb_id = tmdb_data.get("id", "")
            poster = f"https://image.tmdb.org/t/p/original{tmdb_data.get('poster_path')}" if tmdb_data.get("poster_path") else ""
            fanart = f"https://image.tmdb.org/t/p/original{tmdb_data.get('backdrop_path')}" if tmdb_data.get("backdrop_path") else ""
            genres = [g.get("name", "") for g in tmdb_data.get("genres", [])]

            nfo_content = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<movie>\n'
            nfo_content += f'  <title>{self._escape_xml(title)}</title>\n'
            if original_title and original_title != title:
                nfo_content += f'  <originaltitle>{self._escape_xml(original_title)}</originaltitle>\n'
            nfo_content += f'  <plot>{self._escape_xml(plot)}</plot>\n'
            nfo_content += f'  <rating>{rating}</rating>\n'
            nfo_content += f'  <year>{year}</year>\n'
            if runtime:
                nfo_content += f'  <runtime>{runtime}</runtime>\n'
            nfo_content += f'  <tmdbid>{tmdb_id}</tmdbid>\n'
            if poster:
                nfo_content += f'  <thumb>{poster}</thumb>\n'
            if fanart:
                nfo_content += f'  <fanart>{fanart}</fanart>\n'
            for genre in genres:
                nfo_content += f'  <genre>{self._escape_xml(genre)}</genre>\n'
            nfo_content += '</movie>\n'

            tmp = f"{nfo_path}.tmp-{int(time.time() * 1000)}"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(nfo_content)
            os.replace(tmp, nfo_path)
            return True
        except Exception:
            return False

    def _write_tvshow_nfo(self, series_dir: str, tmdb_data: Dict[str, Any], dry_run: bool) -> bool:
        """Write tvshow.nfo file."""
        if dry_run:
            return False
        try:
            nfo_path = os.path.join(series_dir, "tvshow.nfo")
            title = tmdb_data.get("name", "")
            original_title = tmdb_data.get("original_name", "")
            plot = tmdb_data.get("overview", "")
            rating = tmdb_data.get("vote_average", 0)
            year = tmdb_data.get("first_air_date", "")[:4] if tmdb_data.get("first_air_date") else ""
            tmdb_id = tmdb_data.get("id", "")
            poster = f"https://image.tmdb.org/t/p/original{tmdb_data.get('poster_path')}" if tmdb_data.get("poster_path") else ""
            fanart = f"https://image.tmdb.org/t/p/original{tmdb_data.get('backdrop_path')}" if tmdb_data.get("backdrop_path") else ""
            genres = [g.get("name", "") for g in tmdb_data.get("genres", [])]

            nfo_content = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<tvshow>\n'
            nfo_content += f'  <title>{self._escape_xml(title)}</title>\n'
            if original_title and original_title != title:
                nfo_content += f'  <originaltitle>{self._escape_xml(original_title)}</originaltitle>\n'
            nfo_content += f'  <plot>{self._escape_xml(plot)}</plot>\n'
            nfo_content += f'  <rating>{rating}</rating>\n'
            nfo_content += f'  <year>{year}</year>\n'
            nfo_content += f'  <tmdbid>{tmdb_id}</tmdbid>\n'
            if poster:
                nfo_content += f'  <thumb>{poster}</thumb>\n'
            if fanart:
                nfo_content += f'  <fanart>{fanart}</fanart>\n'
            for genre in genres:
                nfo_content += f'  <genre>{self._escape_xml(genre)}</genre>\n'
            nfo_content += '</tvshow>\n'

            tmp = f"{nfo_path}.tmp-{int(time.time() * 1000)}"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(nfo_content)
            os.replace(tmp, nfo_path)
            return True
        except Exception:
            return False

    def _write_episode_nfo_from_tmdb_ep(self, filepath: str, season_num: int, tmdb_ep_data: Dict[str, Any], dry_run: bool) -> bool:
        """Write episode NFO using provided TMDB episode payload (optimized version from ChatGPT)."""
        if dry_run or not tmdb_ep_data:
            return False
        try:
            nfo_path = filepath.rsplit('.', 1)[0] + '.nfo'
            title = tmdb_ep_data.get("name", "")
            plot = tmdb_ep_data.get("overview", "")
            rating = tmdb_ep_data.get("vote_average", 0)
            aired = tmdb_ep_data.get("air_date", "")
            still = f"https://image.tmdb.org/t/p/original{tmdb_ep_data.get('still_path')}" if tmdb_ep_data.get("still_path") else ""
            ep_num = tmdb_ep_data.get("episode_number", 0) or 0

            nfo_content = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<episodedetails>\n'
            nfo_content += f'  <title>{self._escape_xml(title)}</title>\n'
            nfo_content += f'  <season>{season_num}</season>\n'
            nfo_content += f'  <episode>{ep_num}</episode>\n'
            nfo_content += f'  <plot>{self._escape_xml(plot)}</plot>\n'
            nfo_content += f'  <rating>{rating}</rating>\n'
            if aired:
                nfo_content += f'  <aired>{aired}</aired>\n'
            if still:
                nfo_content += f'  <thumb>{still}</thumb>\n'
            nfo_content += '</episodedetails>\n'

            tmp = f"{nfo_path}.tmp-{int(time.time() * 1000)}"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(nfo_content)
            os.replace(tmp, nfo_path)
            return True
        except Exception:
            return False

    def _get_primary_genre(self, tmdb_data: Optional[Dict[str, Any]]) -> str:
        """Extract primary (first) genre from TMDB data, or return 'Uncategorized'."""
        if not tmdb_data:
            return "Uncategorized"
        genres = tmdb_data.get("genres", [])
        if genres and len(genres) > 0:
            return safe_name(genres[0].get("name", "Uncategorized"))
        return "Uncategorized"

    def _get_movie_name_and_year(self, movie: Dict[str, Any], tmdb_api_key: str, tmdb_cache: Dict[str, Any], logger: Any, verbose: bool) -> Tuple[str, Optional[int], Optional[Dict[str, Any]]]:
        """Get movie name and year using TMDB if available."""
        movie_id = movie.get("id")
        tmdb_id = movie.get("tmdb_id")
        original_name = movie.get("name")
        original_year = movie.get("year")
        cache_key = f"movie_{tmdb_id}"
        if tmdb_id and cache_key in tmdb_cache:
            cached = tmdb_cache[cache_key]
            if verbose:
                logger.info("[MOVIE %s] Using cached TMDB data", movie_id)
            return cached.get("title", original_name), cached.get("year", original_year), cached.get("full_data")
        if tmdb_api_key and tmdb_id:
            if verbose:
                logger.info("[MOVIE %s] Looking up TMDB ID %s", movie_id, tmdb_id)
            tmdb_data = self._tmdb_lookup_movie(tmdb_id, tmdb_api_key, logger)
            if tmdb_data:
                title = tmdb_data.get("title", original_name)
                release_date = tmdb_data.get("release_date", "")
                year = int(release_date[:4]) if release_date and len(release_date) >= 4 else original_year
                tmdb_cache[cache_key] = {"title": title, "year": year, "full_data": tmdb_data}
                if verbose:
                    logger.info("[MOVIE %s] TMDB lookup success: '%s' (%s)", movie_id, title, year)
                return title, year, tmdb_data
        if verbose:
            if not tmdb_api_key:
                logger.info("[MOVIE %s] Using original name (no TMDB API key)", movie_id)
            elif not tmdb_id:
                logger.info("[MOVIE %s] Using original name (no TMDB ID): '%s'", movie_id, original_name)
            else:
                logger.info("[MOVIE %s] Using original name (TMDB lookup failed): '%s'", movie_id, original_name)
        return original_name, original_year, None

    def _get_series_name_and_year(self, series: Dict[str, Any], tmdb_api_key: str, tmdb_cache: Dict[str, Any], logger: Any, verbose: bool) -> Tuple[str, Optional[int], Optional[Dict[str, Any]]]:
        """Get series name and year using TMDB if available."""
        series_id = series.get("id")
        tmdb_id = series.get("tmdb_id")
        original_name = series.get("name")
        original_year = series.get("year")
        cache_key = f"series_{tmdb_id}"
        if tmdb_id and cache_key in tmdb_cache:
            cached = tmdb_cache[cache_key]
            if verbose:
                logger.info("[SERIES %s] Using cached TMDB data", series_id)
            return cached.get("name", original_name), cached.get("year", original_year), cached.get("full_data")
        if tmdb_api_key and tmdb_id:
            if verbose:
                logger.info("[SERIES %s] Looking up TMDB ID %s", series_id, tmdb_id)
            tmdb_data = self._tmdb_lookup_series(tmdb_id, tmdb_api_key, logger)
            if tmdb_data:
                name = tmdb_data.get("name", original_name)
                first_air_date = tmdb_data.get("first_air_date", "")
                year = int(first_air_date[:4]) if first_air_date and len(first_air_date) >= 4 else original_year
                tmdb_cache[cache_key] = {"name": name, "year": year, "full_data": tmdb_data}
                if verbose:
                    logger.info("[SERIES %s] TMDB lookup success: '%s' (%s)", series_id, name, year)
                return name, year, tmdb_data
        if verbose:
            if not tmdb_api_key:
                logger.info("[SERIES %s] Using original name (no TMDB API key)", series_id)
            elif not tmdb_id:
                logger.info("[SERIES %s] Using original name (no TMDB ID): '%s'", series_id, original_name)
            else:
                logger.info("[SERIES %s] Using original name (TMDB lookup failed): '%s'", series_id, original_name)
        return original_name, original_year, None

    def _get_vod_stats(self, logger: Any) -> Dict[str, Any]:
        """Get comprehensive statistics on VOD content in database."""
        conn = self._get_db_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Total movies with M3U relations (imported)
                cur.execute("""
                    SELECT COUNT(DISTINCT m.id)
                    FROM vod_movie m
                    INNER JOIN vod_m3umovierelation mr ON m.id = mr.movie_id
                """)
                imported_movies = cur.fetchone()[0]
                
                # Total series with M3U relations (imported)
                cur.execute("""
                    SELECT COUNT(DISTINCT s.id)
                    FROM vod_series s
                    INNER JOIN vod_m3useriesrelation sr ON s.id = sr.series_id
                """)
                imported_series = cur.fetchone()[0]
                
                # Total episodes for imported series
                cur.execute("""
                    SELECT COUNT(DISTINCT e.id)
                    FROM vod_episode e
                    INNER JOIN vod_series s ON e.series_id = s.id
                    INNER JOIN vod_m3useriesrelation sr ON s.id = sr.series_id
                """)
                imported_episodes = cur.fetchone()[0]
                
                # Total in database (all records)
                cur.execute("SELECT COUNT(*) FROM vod_movie")
                total_movies = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM vod_series")
                total_series = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM vod_episode")
                total_episodes = cur.fetchone()[0]
                
                # Provider information
                cur.execute("""
                    SELECT ma.id, ma.name, ma.url
                    FROM m3u_m3uaccount ma
                    ORDER BY ma.name
                """)
                providers = [dict(row) for row in cur.fetchall()]
                
                # Movie count per provider
                cur.execute("""
                    SELECT mr.m3u_account_id, ma.name, COUNT(DISTINCT m.id) as count
                    FROM vod_movie m
                    INNER JOIN vod_m3umovierelation mr ON m.id = mr.movie_id
                    INNER JOIN m3u_m3uaccount ma ON mr.m3u_account_id = ma.id
                    GROUP BY mr.m3u_account_id, ma.name
                    ORDER BY count DESC
                """)
                provider_movie_counts = [dict(row) for row in cur.fetchall()]
                
                # Series count per provider
                cur.execute("""
                    SELECT sr.m3u_account_id, ma.name, COUNT(DISTINCT s.id) as count
                    FROM vod_series s
                    INNER JOIN vod_m3useriesrelation sr ON s.id = sr.series_id
                    INNER JOIN m3u_m3uaccount ma ON sr.m3u_account_id = ma.id
                    GROUP BY sr.m3u_account_id, ma.name
                    ORDER BY count DESC
                """)
                provider_series_counts = [dict(row) for row in cur.fetchall()]
                
                # Duplicate movies (same TMDB ID from multiple providers)
                cur.execute("""
                    SELECT m.tmdb_id, m.name, COUNT(DISTINCT mr.m3u_account_id) as provider_count
                    FROM vod_movie m
                    INNER JOIN vod_m3umovierelation mr ON m.id = mr.movie_id
                    WHERE m.tmdb_id IS NOT NULL AND m.tmdb_id != ''
                    GROUP BY m.tmdb_id, m.name
                    HAVING COUNT(DISTINCT mr.m3u_account_id) > 1
                    ORDER BY provider_count DESC
                """)
                duplicate_movies = [dict(row) for row in cur.fetchall()]
                
                # Duplicate series (same TMDB ID from multiple providers)
                cur.execute("""
                    SELECT s.tmdb_id, s.name, COUNT(DISTINCT sr.m3u_account_id) as provider_count
                    FROM vod_series s
                    INNER JOIN vod_m3useriesrelation sr ON s.id = sr.series_id
                    WHERE s.tmdb_id IS NOT NULL AND s.tmdb_id != ''
                    GROUP BY s.tmdb_id, s.name
                    HAVING COUNT(DISTINCT sr.m3u_account_id) > 1
                    ORDER BY provider_count DESC
                """)
                duplicate_series = [dict(row) for row in cur.fetchall()]
                
                return {
                    "imported_movies": imported_movies,
                    "imported_series": imported_series,
                    "imported_episodes": imported_episodes,
                    "total_movies": total_movies,
                    "total_series": total_series,
                    "total_episodes": total_episodes,
                    "providers": providers,
                    "provider_movie_counts": provider_movie_counts,
                    "provider_series_counts": provider_series_counts,
                    "duplicate_movies": duplicate_movies,
                    "duplicate_series": duplicate_series,
                }
        finally:
            conn.close()

    def _parse_provider_ranking(self, ranking_str: str) -> List[int]:
        """Parse provider ranking string into list of provider IDs."""
        if not ranking_str or not ranking_str.strip():
            return []
        ids = []
        for item in ranking_str.split(','):
            item = item.strip()
            if item.isdigit():
                ids.append(int(item))
        return ids

    def _get_all_movies(self, logger: Any, provider_ranking: List[int], dry_run: bool = False) -> List[Dict[str, Any]]:
        """Fetch all imported movies from database with provider info, handling duplicates by provider ranking."""
        conn = self._get_db_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                    SELECT DISTINCT m.id, m.uuid, m.name, m.year, m.tmdb_id, m.imdb_id, 
                           m.description, m.rating, m.genre, m.duration_secs,
                           mr.m3u_account_id as provider_id,
                           ma.name as provider_name
                    FROM vod_movie m
                    INNER JOIN vod_m3umovierelation mr ON m.id = mr.movie_id
                    INNER JOIN m3u_m3uaccount ma ON mr.m3u_account_id = ma.id
                    ORDER BY m.name
                """
                if dry_run:
                    query += f" LIMIT {DRY_RUN_LIMIT}"
                
                cur.execute(query)
                results = [dict(row) for row in cur.fetchall()]
                
                # Handle duplicates (same TMDB ID from multiple providers)
                if provider_ranking:
                    # Group by TMDB ID
                    tmdb_groups = {}
                    no_tmdb = []
                    for movie in results:
                        tmdb_id = movie.get("tmdb_id")
                        if tmdb_id:
                            if tmdb_id not in tmdb_groups:
                                tmdb_groups[tmdb_id] = []
                            tmdb_groups[tmdb_id].append(movie)
                        else:
                            no_tmdb.append(movie)
                    
                    # For each TMDB ID group, pick highest ranked provider
                    deduplicated = []
                    for tmdb_id, movies in tmdb_groups.items():
                        if len(movies) == 1:
                            deduplicated.append(movies[0])
                        else:
                            # Find highest ranked
                            best_movie = None
                            best_rank = len(provider_ranking) + 1
                            for movie in movies:
                                provider_id = movie.get("provider_id")
                                try:
                                    rank = provider_ranking.index(provider_id)
                                    if rank < best_rank:
                                        best_rank = rank
                                        best_movie = movie
                                except ValueError:
                                    # Provider not in ranking, give it lowest priority
                                    if best_movie is None:
                                        best_movie = movie
                            
                            if best_movie:
                                deduplicated.append(best_movie)
                                if logger:
                                    dup_names = [f"{m.get('provider_name')}(ID:{m.get('provider_id')})" for m in movies]
                                    logger.info("[DEDUP] Movie '%s' (TMDB:%s) from %d providers %s - using %s", 
                                               best_movie.get('name'), tmdb_id, len(movies), dup_names, best_movie.get('provider_name'))
                    
                    deduplicated.extend(no_tmdb)
                    results = deduplicated
                
                logger.info("Fetched %d imported movies from database", len(results))
                return results
        finally:
            conn.close()

    def _get_all_series(self, logger: Any, provider_ranking: List[int], dry_run: bool = False) -> List[Dict[str, Any]]:
        """Fetch all imported series from database with provider info, handling duplicates by provider ranking."""
        conn = self._get_db_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                    SELECT DISTINCT s.id, s.uuid, s.name, s.year, s.tmdb_id, s.imdb_id,
                           s.description, s.rating, s.genre,
                           sr.m3u_account_id as provider_id,
                           ma.name as provider_name
                    FROM vod_series s
                    INNER JOIN vod_m3useriesrelation sr ON s.id = sr.series_id
                    INNER JOIN m3u_m3uaccount ma ON sr.m3u_account_id = ma.id
                    ORDER BY s.name
                """
                if dry_run:
                    query += f" LIMIT {DRY_RUN_LIMIT}"
                
                cur.execute(query)
                results = [dict(row) for row in cur.fetchall()]
                
                # Handle duplicates (same TMDB ID from multiple providers)
                if provider_ranking:
                    # Group by TMDB ID
                    tmdb_groups = {}
                    no_tmdb = []
                    for series in results:
                        tmdb_id = series.get("tmdb_id")
                        if tmdb_id:
                            if tmdb_id not in tmdb_groups:
                                tmdb_groups[tmdb_id] = []
                            tmdb_groups[tmdb_id].append(series)
                        else:
                            no_tmdb.append(series)
                    
                    # For each TMDB ID group, pick highest ranked provider
                    deduplicated = []
                    for tmdb_id, series_list in tmdb_groups.items():
                        if len(series_list) == 1:
                            deduplicated.append(series_list[0])
                        else:
                            # Find highest ranked
                            best_series = None
                            best_rank = len(provider_ranking) + 1
                            for series in series_list:
                                provider_id = series.get("provider_id")
                                try:
                                    rank = provider_ranking.index(provider_id)
                                    if rank < best_rank:
                                        best_rank = rank
                                        best_series = series
                                except ValueError:
                                    # Provider not in ranking, give it lowest priority
                                    if best_series is None:
                                        best_series = series
                            
                            if best_series:
                                deduplicated.append(best_series)
                                if logger:
                                    dup_names = [f"{s.get('provider_name')}(ID:{s.get('provider_id')})" for s in series_list]
                                    logger.info("[DEDUP] Series '%s' (TMDB:%s) from %d providers %s - using %s", 
                                               best_series.get('name'), tmdb_id, len(series_list), dup_names, best_series.get('provider_name'))
                    
                    deduplicated.extend(no_tmdb)
                    results = deduplicated
                
                logger.info("Fetched %d imported series from database", len(results))
                return results
        finally:
            conn.close()

    def _get_series_episodes(self, series_id: int, logger: Any) -> List[Dict[str, Any]]:
        """Fetch all episodes for a series from database."""
        conn = self._get_db_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                    SELECT id, uuid, name, season_number, episode_number,
                           description, air_date, rating, duration_secs,
                           tmdb_id, imdb_id
                    FROM vod_episode
                    WHERE series_id = %s
                    ORDER BY season_number, episode_number
                """
                cur.execute(query, (series_id,))
                results = cur.fetchall()
                return [dict(row) for row in results]
        finally:
            conn.close()

    def _probe_url(self, url: str) -> bool:
        """Check if URL is accessible via HEAD request using optimized session."""
        try:
            response = self.http.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
            return 200 <= response.status_code < 400
        except Exception:
            return False

    def _build_movie_url(self, host: str, uuid: str) -> str:
        """Build proxy URL for movie."""
        return f"http://{normalize_host(host)}/proxy/vod/movie/{uuid}"

    def _build_episode_url(self, host: str, uuid: str) -> str:
        """Build proxy URL for episode."""
        return f"http://{normalize_host(host)}/proxy/vod/episode/{uuid}"

    def _cleanup_stale_files(self, root: str, live_paths: Set[str], logger: Any, dry_run: bool) -> Tuple[int, List[str]]:
        """Remove .strm files and empty directories not in live_paths set."""
        removed_count = 0
        notes = []
        for dirpath, _, files in os.walk(root):
            for filename in files:
                if not filename.lower().endswith(".strm"):
                    continue
                filepath = os.path.normpath(os.path.join(dirpath, filename))
                if filepath not in live_paths:
                    if dry_run:
                        notes.append(f"dry_remove: {filepath}")
                        continue
                    try:
                        os.remove(filepath)
                        removed_count += 1
                        notes.append(f"removed: {filepath}")
                    except Exception as e:
                        notes.append(f"remove_failed: {filepath} ({e})")
        if removed_count > 0 and not dry_run:
            for dirpath, dirnames, filenames in os.walk(root, topdown=False):
                if not dirnames and not filenames:
                    try:
                        os.rmdir(dirpath)
                        notes.append(f"removed empty dir: {dirpath}")
                    except OSError as e:
                        notes.append(f"failed to remove empty dir: {dirpath} ({e})")
        return removed_count, notes

    def _write_movies(self, settings: Dict[str, Any], logger: Any) -> Dict[str, Any]:
        """Write .strm files for movies."""
        root = settings.get("movies_root", "/VODs/movies")
        dry_run = bool(settings.get("dry_run", False))
        verbose = bool(settings.get("verbose", True))
        probe = bool(settings.get("probe_urls", False))
        cleanup = bool(settings.get("cleanup_removed", False))
        write_nfo = bool(settings.get("write_nfo_files", False))
        organize_by_genre = bool(settings.get("organize_by_genre", False))
        dispatch_host = normalize_host(settings.get("dispatcharr_host", "tv.local:9191"))
        tmdb_api_key = settings.get("tmdb_api_key", "").strip()
        provider_ranking_str = settings.get("provider_ranking", "").strip()
        debug_log = bool(settings.get("debug_log", False))

        self._setup_debug_logger(debug_log)
        
        # Wrap logger to write to both django logs and debug file
        logger = DualLogger(logger, self.debug_logger)
        
        logger.info("=== STARTING MOVIE PROCESSING ===")
        logger.info("Root: %s, Dry Run: %s, TMDB: %s, Genre Folders: %s", root, dry_run, 
                   'enabled' if tmdb_api_key else 'disabled', organize_by_genre)

        # Parse provider ranking
        provider_ranking = self._parse_provider_ranking(provider_ranking_str)
        if provider_ranking:
            logger.info("Provider ranking enabled: %s", provider_ranking)

        logger.info("Starting movie .strm generation (root: %s, dry_run: %s)", root, dry_run)
        if tmdb_api_key and not dry_run:
            logger.info("TMDB lookups enabled")
            if write_nfo:
                logger.info("NFO file generation enabled")
        elif tmdb_api_key and dry_run:
            logger.info("TMDB lookups disabled for dry run")

        ensure_dir(root)
        
        # Get and display stats
        try:
            stats = self._get_vod_stats(logger)
            logger.info("Database Stats - Movies: %d imported / %d total", stats['imported_movies'], stats['total_movies'])
            logger.info("Database Stats - Series: %d imported / %d total", stats['imported_series'], stats['total_series'])
            logger.info("Database Stats - Episodes: %d imported / %d total", stats['imported_episodes'], stats['total_episodes'])
        except Exception as e:
            logger.warning("Failed to get database stats: %s", e)
        
        logger.info("Fetching movies from database...")

        rows = self._get_all_movies(logger, provider_ranking, dry_run=dry_run)
        logger.info("Total movies fetched: %d", len(rows))

        manifest = load_manifest(root)
        manifest_files = manifest.get("files", {})
        tmdb_cache = manifest.get("tmdb_cache", {})
        live_paths = set()
        created = updated = skipped = errors = 0
        reasons: Dict[str, int] = {}
        logger.info("Processing %d movies...", len(rows))

        for idx, movie in enumerate(rows):
            movie_id = movie.get("id")
            uuid = movie.get("uuid")
            if idx > 0 and idx % 50 == 0:
                logger.info("Progress: %d/%d movies processed", idx, len(rows))
            if not uuid:
                skipped += 1
                reasons["missing_uuid"] = reasons.get("missing_uuid", 0) + 1
                if verbose:
                    logger.info("[MOVIE %s] skip: missing_uuid", movie_id)
                continue

            tmdb_data = None
            if tmdb_api_key and not dry_run:
                name, year, tmdb_data = self._get_movie_name_and_year(movie, tmdb_api_key, tmdb_cache, logger, verbose)
            else:
                name = movie.get("name")
                year = movie.get("year")

            movie_folder_name = f"{safe_name(name or f'movie-{movie_id}')}{f' ({year})' if year else ''}"
            
            # Determine base path (with or without genre folder)
            if organize_by_genre and tmdb_data:
                genre = self._get_primary_genre(tmdb_data)
                genre_folder = os.path.join(root, genre)
                ensure_dir(genre_folder)
                movie_folder = os.path.join(genre_folder, movie_folder_name)
            else:
                movie_folder = os.path.join(root, movie_folder_name)
            
            ensure_dir(movie_folder)
            filename = f"{movie_folder_name}.strm"
            filepath = os.path.join(movie_folder, filename)
            url = self._build_movie_url(dispatch_host, uuid)

            if probe and not self._probe_url(url):
                skipped += 1
                reasons["probe_failed"] = reasons.get("probe_failed", 0) + 1
                if verbose:
                    logger.info("[MOVIE %s] skip: probe_failed %s", movie_id, url)
                continue

            try:
                changed, reason = write_text_if_changed(filepath, url + "\n", dry_run)
            except Exception as e:
                errors += 1
                reasons["write_failed"] = reasons.get("write_failed", 0) + 1
                if verbose:
                    logger.exception("[MOVIE %s] write_failed %s (%s)", movie_id, filepath, e)
                continue

            normalized_path = os.path.normpath(filepath)
            live_paths.add(normalized_path)

            if changed and reason == "written":
                if normalized_path in manifest_files:
                    updated += 1
                else:
                    created += 1
                manifest_files[normalized_path] = {"uuid": uuid, "type": "movie", "id": movie_id}
                if write_nfo and tmdb_data:
                    self._write_movie_nfo(filepath, tmdb_data, dry_run)
                if verbose:
                    logger.info("[MOVIE %s] wrote %s", movie_id, filepath)
            else:
                skipped += 1
                reasons[reason] = reasons.get(reason, 0) + 1
                if verbose and reason != 'unchanged':
                    logger.info("[MOVIE %s] skip %s (%s)", movie_id, filepath, reason)

        manifest["files"] = manifest_files
        manifest["tmdb_cache"] = tmdb_cache
        if not dry_run:
            save_manifest(root, manifest)
        logger.info("Movie processing complete: created=%d, updated=%d, skipped=%d, errors=%d", created, updated, skipped, errors)

        removed = 0
        if cleanup:
            logger.info("Running cleanup to remove stale files...")
            removed, notes = self._cleanup_stale_files(root, live_paths, logger, dry_run)
            logger.info("Cleanup complete. Removed %d stale file(s).", removed)
            if verbose and notes:
                for note in notes[:20]:
                    logger.info("[MOVIE CLEANUP] %s", note)
                if len(notes) > 20:
                    logger.info("[MOVIE CLEANUP] ...and %d more notes.", len(notes) - 20)

        return {"status": "ok", "scope": "movies", "created": created, "updated": updated, "skipped": skipped, "errors": errors, "removed": removed, "dry_run": dry_run, "count_input": len(rows), "skip_reasons": reasons}

    def _write_series(self, settings: Dict[str, Any], logger: Any) -> Dict[str, Any]:
        """Write .strm files for series episodes with optimized TMDB batching."""
        root = settings.get("series_root", "/VODs/series")
        dry_run = bool(settings.get("dry_run", False))
        verbose = bool(settings.get("verbose", True))
        probe = bool(settings.get("probe_urls", False))
        cleanup = bool(settings.get("cleanup_removed", False))
        write_nfo = bool(settings.get("write_nfo_files", False))
        organize_by_genre = bool(settings.get("organize_by_genre", False))
        dispatch_host = normalize_host(settings.get("dispatcharr_host", "tv.local:9191"))
        tmdb_api_key = settings.get("tmdb_api_key", "").strip()
        provider_ranking_str = settings.get("provider_ranking", "").strip()
        debug_log = bool(settings.get("debug_log", False))

        self._setup_debug_logger(debug_log)
        
        # Wrap logger to write to both django logs and debug file
        logger = DualLogger(logger, self.debug_logger)
        
        logger.info("=== STARTING SERIES PROCESSING ===")
        logger.info("Root: %s, Dry Run: %s, TMDB: %s, Genre Folders: %s", root, dry_run, 
                   'enabled' if tmdb_api_key else 'disabled', organize_by_genre)

        # Parse provider ranking
        provider_ranking = self._parse_provider_ranking(provider_ranking_str)
        if provider_ranking:
            logger.info("Provider ranking enabled: %s", provider_ranking)

        logger.info("Starting series .strm generation (root: %s, dry_run: %s)", root, dry_run)
        if tmdb_api_key and not dry_run:
            logger.info("TMDB lookups enabled with optimized season batching")
            if write_nfo:
                logger.info("NFO file generation enabled")
        elif tmdb_api_key and dry_run:
            logger.info("TMDB lookups disabled for dry run")

        ensure_dir(root)
        
        # Get and display stats
        try:
            stats = self._get_vod_stats(logger)
            logger.info("Database Stats - Movies: %d imported / %d total", stats['imported_movies'], stats['total_movies'])
            logger.info("Database Stats - Series: %d imported / %d total", stats['imported_series'], stats['total_series'])
            logger.info("Database Stats - Episodes: %d imported / %d total", stats['imported_episodes'], stats['total_episodes'])
        except Exception as e:
            logger.warning("Failed to get database stats: %s", e)
        
        logger.info("Fetching series from database...")

        series_rows = self._get_all_series(logger, provider_ranking, dry_run=dry_run)
        logger.info("Total series fetched: %d", len(series_rows))

        manifest = load_manifest(root)
        manifest_files = manifest.get("files", {})
        tmdb_cache = manifest.get("tmdb_cache", {})
        live_paths = set()
        created = updated = skipped = errors = 0
        reasons: Dict[str, int] = {}
        logger.info("Processing %d series...", len(series_rows))

        for idx, series in enumerate(series_rows):
            series_id = series.get("id")
            series_tmdb_id = series.get("tmdb_id")
            if idx > 0 and idx % 20 == 0:
                logger.info("Progress: %d/%d series processed", idx, len(series_rows))

            tmdb_data = None
            if tmdb_api_key and not dry_run:
                series_name, series_year, tmdb_data = self._get_series_name_and_year(series, tmdb_api_key, tmdb_cache, logger, verbose)
            else:
                series_name = series.get("name")
                series_year = series.get("year")

            series_dir_name = f"{safe_name(series_name or f'series-{series_id}')}{f' ({series_year})' if series_year else ''}"
            
            # Determine base path (with or without genre folder)
            if organize_by_genre and tmdb_data:
                genre = self._get_primary_genre(tmdb_data)
                genre_folder = os.path.join(root, genre)
                ensure_dir(genre_folder)
                series_dir = os.path.join(genre_folder, series_dir_name)
            else:
                series_dir = os.path.join(root, series_dir_name)

            try:
                episodes_data = self._get_series_episodes(series_id, logger)
            except Exception as e:
                errors += 1
                if verbose:
                    logger.exception("[SERIES %s] episodes fetch error: %s", series_id, e)
                continue

            if not episodes_data:
                skipped += 1
                reasons["no_episodes"] = reasons.get("no_episodes", 0) + 1
                if verbose:
                    logger.info("[SERIES %s] skip: no episodes found (%s)", series_id, series_name)
                continue

            ensure_dir(series_dir)
            if write_nfo and tmdb_data:
                self._write_tvshow_nfo(series_dir, tmdb_data, dry_run)

            # Group episodes by season for optimized TMDB fetching
            seasons: Dict[int, List[Dict[str, Any]]] = {}
            for episode in episodes_data:
                season_num = int(episode.get("season_number", 0) or 0)
                seasons.setdefault(season_num, []).append(episode)

            for season_num in sorted(seasons.keys()):
                season_dir = os.path.join(series_dir, f"Season {season_num:02d}")
                ensure_dir(season_dir)
                sorted_episodes = sorted(seasons[season_num], key=lambda e: int(e.get("episode_number", 0) or 0))

                # Fetch TMDB season once (optimized batching from ChatGPT version)
                ep_map: Optional[Dict[int, Dict[str, Any]]] = None
                if write_nfo and tmdb_api_key and series_tmdb_id:
                    ep_map = self._tmdb_fetch_season_map(series_tmdb_id, season_num, tmdb_api_key, tmdb_cache, logger)

                for episode in sorted_episodes:
                    ep_uuid = episode.get("uuid")
                    ep_num = int(episode.get("episode_number", 0) or 0)
                    ep_title = episode.get("name") or f"E{ep_num:02d}"
                    if not ep_uuid:
                        skipped += 1
                        reasons["missing_uuid"] = reasons.get("missing_uuid", 0) + 1
                        if verbose:
                            logger.info("[SERIES %s] skip S%02dE%02d: missing_uuid (%s)", series_id, season_num, ep_num, ep_title)
                        continue

                    url = self._build_episode_url(dispatch_host, ep_uuid)
                    if probe and not self._probe_url(url):
                        skipped += 1
                        reasons["probe_failed"] = reasons.get("probe_failed", 0) + 1
                        if verbose:
                            logger.info("[SERIES %s] skip S%02dE%02d: probe_failed %s", series_id, season_num, ep_num, url)
                        continue

                    filename = f"{series_dir_name} - S{season_num:02d}E{ep_num:02d} - {safe_name(ep_title)}.strm"
                    filepath = os.path.join(season_dir, filename)
                    
                    try:
                        changed, reason = write_text_if_changed(filepath, url + "\n", dry_run)
                    except Exception as e:
                        errors += 1
                        reasons["write_failed"] = reasons.get("write_failed", 0) + 1
                        if verbose:
                            logger.exception("[SERIES %s] write_failed %s (%s)", series_id, filepath, e)
                        continue

                    normalized_path = os.path.normpath(filepath)
                    live_paths.add(normalized_path)

                    if changed and reason == "written":
                        if normalized_path in manifest_files:
                            updated += 1
                        else:
                            created += 1
                        manifest_files[normalized_path] = {"uuid": ep_uuid, "type": "episode", "series_id": series_id, "season": season_num, "episode": ep_num}
                        
                        # Optimized episode NFO from pre-fetched season map (no per-episode HTTP)
                        if write_nfo and ep_map:
                            tmdb_ep = ep_map.get(ep_num)
                            if tmdb_ep:
                                self._write_episode_nfo_from_tmdb_ep(filepath, season_num, tmdb_ep, dry_run)
                            else:
                                if verbose:
                                    logger.info("[SERIES %s] TMDB episode missing in season map S%02dE%02d", series_id, season_num, ep_num)

                        if verbose:
                            logger.info("[SERIES %s] wrote %s", series_id, filepath)
                    else:
                        skipped += 1
                        reasons[reason] = reasons.get(reason, 0) + 1
                        if verbose and reason != 'unchanged':
                            logger.info("[SERIES %s] skip %s (%s)", series_id, filepath, reason)

        manifest["files"] = manifest_files
        manifest["tmdb_cache"] = tmdb_cache
        if not dry_run:
            save_manifest(root, manifest)
        logger.info("Series processing complete: created=%d, updated=%d, skipped=%d, errors=%d", created, updated, skipped, errors)

        removed = 0
        if cleanup:
            logger.info("Running cleanup to remove stale files...")
            removed, notes = self._cleanup_stale_files(root, live_paths, logger, dry_run)
            logger.info("Cleanup complete. Removed %d stale file(s).", removed)
            if verbose and notes:
                for note in notes[:20]:
                    logger.info("[SERIES CLEANUP] %s", note)
                if len(notes) > 20:
                    logger.info("[SERIES CLEANUP] ...and %d more notes.", len(notes) - 20)

        return {"status": "ok", "scope": "series", "created": created, "updated": updated, "skipped": skipped, "errors": errors, "removed": removed, "dry_run": dry_run, "count_input": len(series_rows), "skip_reasons": reasons}

    def run(self, action: str, params: dict, context: dict) -> Dict[str, Any]:
        """Main entry point for plugin actions."""
        logger = context.get("logger")
        settings = context.get("settings", {})

        logger.info("VOD STRM Plugin v%s - Action: %s", self.version, action)

        # Auto-save settings and restart scheduler whenever any action runs
        self._save_settings(settings)
        self._start_background_scheduler(settings)

        try:
            logger.info("Connecting to database...")
            # Test database connection
            conn = self._get_db_connection()
            conn.close()
            logger.info("Database connection successful")

            if action == "show_stats":
                stats = self._get_vod_stats(logger)
                
                # Build provider list
                provider_lines = []
                for p in stats['providers']:
                    provider_lines.append(f"  • ID {p['id']}: {p['name']}")
                providers_text = "\n".join(provider_lines) if provider_lines else "  None found"
                
                # Build provider breakdown
                movie_breakdown = []
                for p in stats['provider_movie_counts']:
                    movie_breakdown.append(f"  • {p['name']} (ID {p['m3u_account_id']}): {p['count']} movies")
                movie_breakdown_text = "\n".join(movie_breakdown) if movie_breakdown else "  None"
                
                series_breakdown = []
                for p in stats['provider_series_counts']:
                    series_breakdown.append(f"  • {p['name']} (ID {p['m3u_account_id']}): {p['count']} series")
                series_breakdown_text = "\n".join(series_breakdown) if series_breakdown else "  None"
                
                # Build duplicate info
                dup_movie_count = len(stats['duplicate_movies'])
                dup_series_count = len(stats['duplicate_series'])
                dup_movies_text = f"{dup_movie_count} movies with duplicates"
                dup_series_text = f"{dup_series_count} series with duplicates"
                
                if dup_movie_count > 0:
                    top_dup_movies = stats['duplicate_movies'][:5]
                    dup_movies_list = []
                    for m in top_dup_movies:
                        dup_movies_list.append(f"  • {m['name']} (TMDB:{m['tmdb_id']}): {m['provider_count']} providers")
                    if dup_movie_count > 5:
                        dup_movies_list.append(f"  ... and {dup_movie_count - 5} more")
                    dup_movies_text += "\n" + "\n".join(dup_movies_list)
                
                if dup_series_count > 0:
                    top_dup_series = stats['duplicate_series'][:5]
                    dup_series_list = []
                    for s in top_dup_series:
                        dup_series_list.append(f"  • {s['name']} (TMDB:{s['tmdb_id']}): {s['provider_count']} providers")
                    if dup_series_count > 5:
                        dup_series_list.append(f"  ... and {dup_series_count - 5} more")
                    dup_series_text += "\n" + "\n".join(dup_series_list)
                
                message = f"""Database Statistics:

CONTENT COUNTS:
Movies: {stats['imported_movies']} imported / {stats['total_movies']} total
Series: {stats['imported_series']} imported / {stats['total_series']} total
Episodes: {stats['imported_episodes']} imported / {stats['total_episodes']} total

PROVIDERS:
{providers_text}

CONTENT BY PROVIDER:
Movies:
{movie_breakdown_text}

Series:
{series_breakdown_text}

DUPLICATES (same TMDB ID from multiple providers):
Movies: {dup_movies_text}

Series: {dup_series_text}

Note: "Imported" means VODs with active M3U provider relations.
Use provider IDs in the "Provider Priority" setting to control which provider is used for duplicates.
Only imported VODs will be processed when creating .strm files."""
                return {"status": "success", "message": message}

            if action == "write_movies":
                return self._write_movies(settings, logger)
            if action == "write_series":
                return self._write_series(settings, logger)
            return {"status": "error", "message": f"Unknown action: {action}"}

        except psycopg2.Error as e:
            logger.exception("Database error during plugin execution: %s", e)
            return {"status": "error", "message": f"Database Error: {e}"}
        except Exception as e:
            logger.exception("An unexpected error occurred during plugin execution.")
            return {"status": "error", "message": f"An unexpected error occurred: {e}"}


# Plugin exports
plugin = Plugin()
fields = Plugin.fields
actions = Plugin.actions
