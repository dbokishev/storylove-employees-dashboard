import csv
import io
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz
from dotenv import load_dotenv
from flask import Flask, jsonify, make_response, redirect, render_template, request, url_for

from services.google_sheets import get_sheets_service
from utils.helpers import (
    calculate_analytics,
    calculate_employee_month_analytics,
    calculate_timesheet,
    calculate_today_summary,
)

load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-key")
app.config["DEBUG"] = _env_bool("DEBUG", False)

ALMATY_TZ = pytz.timezone("Asia/Almaty")


def _month_bounds(month_str: Optional[str]):
    try:
        if month_str:
            y, m = month_str.split("-")
            year, month = int(y), int(m)
        else:
            raise ValueError
    except ValueError:
        now = datetime.now(ALMATY_TZ)
        year, month = now.year, now.month
    first = date(year, month, 1)
    if month == 12:
        last = date(year, 12, 31)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    return first, last


def _parse_date_param(value: Optional[str]) -> date:
    if value:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            pass
    return datetime.now(ALMATY_TZ).date()


def _employees_payload(users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for user in users:
        out.append(
            {
                "userId": str(user.get("user_id", "")).strip(),
                "name": user.get("name", ""),
                "username": user.get("username", ""),
                "fullName": user.get("full_name") or user.get("name") or "Без имени",
            }
        )
    out.sort(key=lambda e: str(e.get("fullName", "")))
    return out


@app.route("/")
def index():
    return render_template("today.html")


@app.route("/employees")
def employees_page():
    return render_template("employees.html")


@app.route("/analytics")
def analytics_redirect():
    return redirect(url_for("analytics_employee_page"))


@app.route("/analytics/employee")
def analytics_employee_page():
    return render_template("analytics_employee.html")


@app.route("/analytics/company")
def analytics_company_page():
    return render_template("analytics_company.html")


@app.route("/api/attendance/today")
def api_today():
    try:
        d = _parse_date_param(request.args.get("date"))
        service = get_sheets_service()
        users = service.get_users()
        logs = service.get_logs(date_from=d, date_to=d)
        schedule = service.get_schedule(date_from=d, date_to=d)
        holidays = service.get_holidays(date_from=d, date_to=d)
        payload = calculate_today_summary(
            users, logs, schedule, holidays, d, datetime.now(ALMATY_TZ)
        )
        return jsonify(
            {
                "success": True,
                "data": payload,
                "targetDate": d.isoformat(),
                "lastUpdate": datetime.now(ALMATY_TZ).isoformat(),
            }
        )
    except Exception as error:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(error)}), 500


@app.route("/api/attendance/analytics")
def api_analytics():
    try:
        month_str = request.args.get("month") or datetime.now(ALMATY_TZ).strftime("%Y-%m")
        user_id = request.args.get("user_id") or request.args.get("userId")
        first_day, last_day = _month_bounds(month_str)
        service = get_sheets_service()
        users = service.get_users()
        logs = service.get_logs(date_from=first_day, date_to=last_day)
        schedule = service.get_schedule(date_from=first_day, date_to=last_day)
        holidays = service.get_holidays(date_from=first_day, date_to=last_day)
        employee_directory = service.get_employee_directory()

        analytics = calculate_analytics(
            users, logs, schedule, holidays, first_day, last_day
        )
        timesheet = calculate_timesheet(
            users,
            logs,
            schedule,
            holidays,
            employee_directory,
            first_day,
            last_day,
        )
        result: Dict[str, Any] = {
            "month": month_str,
            "stats": analytics.get("stats", {}),
            "calendar": analytics.get("calendar", {}),
            "timesheet": timesheet,
            "employees": _employees_payload(users),
        }
        if user_id:
            result["employeeAnalytics"] = calculate_employee_month_analytics(
                users,
                logs,
                schedule,
                holidays,
                first_day,
                last_day,
                user_id,
                datetime.now(ALMATY_TZ),
            )
        return jsonify({"success": True, "data": result})
    except Exception as error:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(error)}), 500


def _write_company_timesheet_csv(month_str: Optional[str]):
    service = get_sheets_service()
    first_day, last_day = _month_bounds(month_str)
    users = service.get_users()
    logs = service.get_logs(date_from=first_day, date_to=last_day)
    schedule = service.get_schedule(date_from=first_day, date_to=last_day)
    holidays = service.get_holidays(date_from=first_day, date_to=last_day)
    employee_directory = service.get_employee_directory()
    timesheet = calculate_timesheet(
        users, logs, schedule, holidays, employee_directory, first_day, last_day
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "ФИО",
            "Должность",
            "Рабочих_дней",
            "Опозданий",
            "Прогулов",
            "Всего_часов",
            "Средних_часов_день",
            "Процент_нормы",
        ]
    )
    for row in timesheet["rows"]:
        writer.writerow(
            [
                row["fullName"],
                row["position"],
                row["workDaysFact"],
                row["lateCount"],
                row["absences"],
                row["totalHours"],
                row["avgHoursPerDay"],
                f'{row["completionPercent"]}%',
            ]
        )
    return output.getvalue(), first_day


def _write_employee_csv(month_str: Optional[str], user_id: str):
    service = get_sheets_service()
    first_day, last_day = _month_bounds(month_str)
    users = service.get_users()
    logs = service.get_logs(date_from=first_day, date_to=last_day)
    schedule = service.get_schedule(date_from=first_day, date_to=last_day)
    holidays = service.get_holidays(date_from=first_day, date_to=last_day)
    data = calculate_employee_month_analytics(
        users, logs, schedule, holidays, first_day, last_day, user_id, datetime.now(ALMATY_TZ)
    )
    if data.get("error"):
        return None, None
    emp = data.get("employee") or {}
    stats = data.get("stats") or {}
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ФИО", emp.get("fullName", "")])
    writer.writerow(["User ID", emp.get("userId", "")])
    writer.writerow(["Месяц", first_day.strftime("%Y-%m")])
    writer.writerow([])
    writer.writerow(
        [
            "Опозданий",
            "Рабочих_дней_факт",
            "План_дней",
            "Прогулов",
            "Часов_отработано",
            "Норма_часов",
            "Процент_нормы",
        ]
    )
    writer.writerow(
        [
            stats.get("lateCount"),
            stats.get("workDays"),
            stats.get("planDays"),
            stats.get("absences"),
            stats.get("workHours"),
            stats.get("normHours"),
            f'{stats.get("completionPercent")}%',
        ]
    )
    writer.writerow([])
    writer.writerow(
        [
            "Дата",
            "Статус",
            "Приход",
            "Уход",
            "Часы",
            "Недоработка",
            "Примечание",
        ]
    )
    for day in data.get("days", []):
        note = day.get("holidayName") or day.get("scheduleNote") or ""
        writer.writerow(
            [
                day.get("date"),
                day.get("status"),
                day.get("checkIn") or "",
                day.get("checkOut") or "",
                day.get("hoursWorked") if day.get("hoursWorked") is not None else "",
                day.get("underworkedBy") or "",
                note,
            ]
        )
    safe_name = "".join(
        c if c.isalnum() or c in " -_" else "_" for c in str(emp.get("fullName", "employee"))
    )
    return output.getvalue(), safe_name


@app.route("/api/attendance/export/company.csv")
def export_company_csv():
    month_str = request.args.get("month") or datetime.now(ALMATY_TZ).strftime("%Y-%m")
    try:
        text, first_day = _write_company_timesheet_csv(month_str)
        resp = make_response(text)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="company-{first_day.strftime("%Y-%m")}.csv"'
        )
        return resp
    except Exception as error:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(error)}), 500


@app.route("/api/attendance/export/employee.csv")
def export_employee_csv():
    month_str = request.args.get("month") or datetime.now(ALMATY_TZ).strftime("%Y-%m")
    user_id = request.args.get("user_id") or request.args.get("userId") or ""
    if not user_id:
        return jsonify({"success": False, "error": "user_id is required"}), 400
    try:
        text, safe_name = _write_employee_csv(month_str, user_id)
        if text is None:
            return jsonify({"success": False, "error": "Employee not found"}), 404
        first_day, _ = _month_bounds(month_str)
        resp = make_response(text)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="employee-{safe_name}-{first_day.strftime("%Y-%m")}.csv"'
        )
        return resp
    except Exception as error:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(error)}), 500


@app.route("/api/attendance/timesheet.csv")
def export_timesheet_legacy():
    return export_company_csv()


@app.route("/api/employees")
def api_employees():
    try:
        service = get_sheets_service()
        users = service.get_users()
        return jsonify({"success": True, "data": {"employees": _employees_payload(users)}})
    except Exception as error:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(error)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(
        host="127.0.0.1",
        port=port,
        debug=app.config.get("DEBUG", False),
    )
