import csv
import io
import os
from datetime import date, datetime, timedelta
from typing import Optional

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

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-key")

ALMATY_TZ = pytz.timezone("Asia/Almaty")


def _month_bounds(month_str: str):
    try:
        y, m = month_str.split("-")
        year, month = int(y), int(m)
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
        data = calculate_today_summary(
            users, logs, schedule, holidays, d, datetime.now(ALMATY_TZ)
        )
        return jsonify({"success": True, "data": data})
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
        result = {
            "month": month_str,
            "analytics": calculate_analytics(
                users, logs, schedule, holidays, first_day, last_day
            ),
            "timesheet": calculate_timesheet(
                users,
                logs,
                schedule,
                holidays,
                employee_directory,
                first_day,
                last_day,
            ),
        }
        if user_id:
            result["employee"] = calculate_employee_month_analytics(
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


def _write_company_timesheet_csv(month_str: str):
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


def _write_employee_csv(month_str: str, user_id: str):
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
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Дата", "День", "Статус", "Приход", "Уход", "Часы"])
    emp = data.get("employee") or {}
    name = emp.get("full_name") or emp.get("name") or "employee"
    for day in data.get("days", []):
        writer.writerow(
            [
                day.get("date"),
                day.get("weekday"),
                day.get("status"),
                day.get("checkIn"),
                day.get("checkOut"),
                day.get("hoursWorked"),
            ]
        )
    return output.getvalue(), name


@app.route("/api/attendance/export/company.csv")
def export_company_csv():
    month_str = request.args.get("month") or datetime.now(ALMATY_TZ).strftime("%Y-%m")
    try:
        text, first_day = _write_company_timesheet_csv(month_str)
        resp = make_response(text)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="timesheet-company-{month_str}.csv"'
        )
        return resp
    except Exception as error:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(error)}), 500


@app.route("/api/attendance/export/employee.csv")
def export_employee_csv():
    month_str = request.args.get("month") or datetime.now(ALMATY_TZ).strftime("%Y-%m")
    user_id = request.args.get("user_id") or request.args.get("userId") or ""
    if not user_id:
        return jsonify({"success": False, "error": "user_id required"}), 400
    try:
        text, name = _write_employee_csv(month_str, user_id)
        if text is None:
            return jsonify({"success": False, "error": "user_not_found"}), 404
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in str(name))
        resp = make_response(text)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="timesheet-employee-{safe}-{month_str}.csv"'
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
        return jsonify({"success": True, "data": service.get_users()})
    except Exception as error:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(error)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    app.run(host="127.0.0.1", port=port, debug=True)
