import json
import os
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _spreadsheet_id() -> str:
    return (os.getenv("SPREADSHEET_ID") or os.getenv("GOOGLE_SHEET_ID") or "").strip()


def _credentials_path() -> str:
    return (
        os.getenv("GOOGLE_CREDENTIALS_PATH")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or "credentials.json"
    ).strip()


class GoogleSheetsService:
    """
    Чтение Google Sheets (как в прежней версии проекта): кэш TTL, SPREADSHEET_ID,
    плюс совместимость с GOOGLE_SHEET_ID и GOOGLE_CREDENTIALS (JSON в env).
    """

    def __init__(
        self,
        spreadsheet_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
    ) -> None:
        self.spreadsheet_id = spreadsheet_id or _spreadsheet_id()
        if not self.spreadsheet_id:
            raise RuntimeError("Задайте SPREADSHEET_ID или GOOGLE_SHEET_ID в .env")
        self.credentials = self._load_credentials(credentials_path or _credentials_path())
        self.client = gspread.authorize(self.credentials)
        self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
        self._cache: Dict[str, tuple] = {}
        self._cache_ttl = 120

    @staticmethod
    def _load_credentials(credentials_path: Optional[str]) -> Credentials:
        creds_json = os.getenv("GOOGLE_CREDENTIALS")
        if creds_json:
            return Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
        path = credentials_path or ""
        if path and os.path.isfile(path):
            return Credentials.from_service_account_file(path, scopes=SCOPES)
        raise RuntimeError(
            "Укажите credentials: файл (GOOGLE_CREDENTIALS_PATH / credentials.json) "
            "или переменную GOOGLE_CREDENTIALS с JSON сервисного аккаунта."
        )

    def _get_cached(self, key: str, fetch_func: Callable[[], Any]) -> Any:
        now = datetime.now().timestamp()
        if key in self._cache:
            data, ts = self._cache[key]
            if now - ts < self._cache_ttl:
                return data
        data = fetch_func()
        self._cache[key] = (data, now)
        return data

    @staticmethod
    def _safe_records(worksheet: gspread.Worksheet) -> List[Dict[str, Any]]:
        records = worksheet.get_all_records()
        return [r for r in records if isinstance(r, dict)]

    def _filter_by_date(
        self,
        all_records: List[Dict[str, Any]],
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        if not date_from and not date_to:
            return all_records
        filtered: List[Dict[str, Any]] = []
        for record in all_records:
            raw = record.get("date") or record.get("Date") or record.get("Дата")
            date_str = str(raw or "").strip()[:10]
            if not date_str:
                continue
            try:
                record_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if date_from and record_date < date_from:
                continue
            if date_to and record_date > date_to:
                continue
            filtered.append(record)
        return filtered

    def _ws_title(self, env_key: str, default: str) -> str:
        return os.getenv(env_key, default)

    def get_users(self) -> List[Dict[str, Any]]:
        title = self._ws_title("SHEET_USERS", "Users")

        def fetch() -> List[Dict[str, Any]]:
            return self._safe_records(self.spreadsheet.worksheet(title))

        return self._get_cached(f"users_{title}", fetch)

    def get_logs(
        self, date_from: Optional[date] = None, date_to: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        title = self._ws_title("SHEET_LOGS", "Logs")

        def fetch() -> List[Dict[str, Any]]:
            return self._filter_by_date(
                self._safe_records(self.spreadsheet.worksheet(title)),
                date_from,
                date_to,
            )

        return self._get_cached(f"logs_{title}_{date_from}_{date_to}", fetch)

    def get_schedule(
        self, date_from: Optional[date] = None, date_to: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        title = self._ws_title("SHEET_SCHEDULE", "Schedule")

        def fetch() -> List[Dict[str, Any]]:
            try:
                records = self._safe_records(self.spreadsheet.worksheet(title))
            except gspread.exceptions.WorksheetNotFound:
                return []
            return self._filter_by_date(records, date_from, date_to)

        return self._get_cached(f"schedule_{title}_{date_from}_{date_to}", fetch)

    def get_holidays(
        self, date_from: Optional[date] = None, date_to: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        title = self._ws_title("SHEET_HOLIDAYS", "Holidays")

        def fetch() -> List[Dict[str, Any]]:
            try:
                records = self._safe_records(self.spreadsheet.worksheet(title))
            except gspread.exceptions.WorksheetNotFound:
                return []
            return self._filter_by_date(records, date_from, date_to)

        return self._get_cached(f"holidays_{title}_{date_from}_{date_to}", fetch)

    def get_employee_directory(self) -> List[Dict[str, Any]]:
        title = self._ws_title("SHEET_EMPLOYEES", "Employees")

        def fetch() -> List[Dict[str, Any]]:
            try:
                return self._safe_records(self.spreadsheet.worksheet(title))
            except gspread.exceptions.WorksheetNotFound:
                return []

        return self._get_cached(f"employees_{title}", fetch)


_sheets_service: Optional[GoogleSheetsService] = None


def get_sheets_service() -> GoogleSheetsService:
    global _sheets_service
    if _sheets_service is None:
        _sheets_service = GoogleSheetsService()
    return _sheets_service
