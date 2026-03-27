from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

WORK_START_TIME = time(11, 0)
FULL_DAY_MINUTES = 9 * 60
NORM_DAY_MINUTES = 8 * 60


def _parse_time(value: Any) -> Optional[time]:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def is_late(check_in_str: Any) -> bool:
    parsed = _parse_time(check_in_str)
    return bool(parsed and parsed > WORK_START_TIME)


def calculate_hours(check_in_str: Any, check_out_str: Any) -> Optional[str]:
    check_in_time = _parse_time(check_in_str)
    check_out_time = _parse_time(check_out_str)
    if not check_in_time or not check_out_time:
        return None
    start = datetime.combine(date.today(), check_in_time)
    end = datetime.combine(date.today(), check_out_time)
    if end < start:
        end += timedelta(days=1)
    delta = end - start
    total_minutes = int(delta.total_seconds() // 60)
    h, m = divmod(total_minutes, 60)
    return f"{h}:{m:02d}"


def _hours_to_minutes(hours_str: Any) -> int:
    if not hours_str or hours_str == "N/A":
        return 0
    parts = str(hours_str).split(":")
    if len(parts) < 2:
        return 0
    try:
        h = int(parts[0])
        m = int(parts[1])
        return h * 60 + m
    except ValueError:
        return 0


def _minutes_to_hours_str(total: int) -> str:
    if total <= 0:
        return "0:00"
    h, m = divmod(total, 60)
    return f"{h}:{m:02d}"


def _worked_minutes(check_in_str: Any, check_out_str: Any) -> int:
    return _hours_to_minutes(calculate_hours(check_in_str, check_out_str))


def _is_weekend(d: date) -> bool:
    return d.weekday() >= 5


def _norm_date_key(raw: Any) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    return s[:10]


def _build_holidays_map(holidays: List[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for h in holidays:
        ds = _norm_date_key(h.get("date") or h.get("Date"))
        if not ds:
            continue
        name = str(h.get("name") or h.get("Name") or h.get("title") or "Праздник").strip()
        out[ds] = name
    return out


def _build_schedule_index(schedule: List[Dict[str, Any]]) -> Dict[Tuple[str, str], bool]:
    idx: Dict[Tuple[str, str], bool] = {}
    for item in schedule:
        fn = str(item.get("full_name") or item.get("fullName") or item.get("ФИО") or "").strip()
        ds = _norm_date_key(item.get("date") or item.get("Date"))
        if fn and ds:
            idx[(fn, ds)] = True
    return idx


def _coerce_log_rows(raw_logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Приводим логи к виду с check_in / check_out (первая отметка входа, последняя выхода)."""
    flat: Dict[Tuple[str, str], Dict[str, Any]] = {}
    stream: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)

    for row in raw_logs:
        uid = str(row.get("user_id") or row.get("userId") or row.get("User ID") or "").strip()
        ds = _norm_date_key(row.get("date") or row.get("Date"))
        if not uid or not ds:
            continue
        cin = row.get("check_in") or row.get("check_in_time") or row.get("time_in") or row.get("Приход")
        cout = row.get("check_out") or row.get("check_out_time") or row.get("time_out") or row.get("Уход")
        if cin or cout:
            flat[(uid, ds)] = {"user_id": uid, "date": ds, "check_in": cin or "", "check_out": cout or ""}
            continue
        t = row.get("time") or row.get("Time") or row.get("timestamp")
        ev = str(row.get("event") or row.get("type") or row.get("action") or "").lower()
        if not t:
            continue
        if any(x in ev for x in ("in", "check_in", "приход", "вход")):
            stream[(uid, ds)].append(("in", str(t).strip()))
        elif any(x in ev for x in ("out", "check_out", "уход", "выход")):
            stream[(uid, ds)].append(("out", str(t).strip()))

    for key, pairs in stream.items():
        ins = [p[1] for p in pairs if p[0] == "in"]
        outs = [p[1] for p in pairs if p[0] == "out"]
        if key in flat:
            continue
        flat[key] = {
            "user_id": key[0],
            "date": key[1],
            "check_in": ins[0] if ins else "",
            "check_out": outs[-1] if outs else "",
        }

    return list(flat.values())


def _build_user_day_logs(logs: List[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    coerced = _coerce_log_rows(logs)
    by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in coerced:
        uid = str(row.get("user_id", "")).strip()
        ds = _norm_date_key(row.get("date"))
        if uid and ds:
            by_key[(uid, ds)].append(row)
    return by_key


def _extract_check_times(day_rows: List[Dict[str, Any]]) -> Tuple[str, str]:
    if not day_rows:
        return "", ""
    if len(day_rows) == 1:
        r = day_rows[0]
        return str(r.get("check_in") or "").strip(), str(r.get("check_out") or "").strip()
    check_in = str(day_rows[0].get("check_in") or "").strip()
    check_out = str(day_rows[-1].get("check_out") or "").strip()
    return check_in, check_out


def _resolve_day_status(
    check_in_str: Any, check_out_str: Any, target_date: date, now_almaty: datetime
) -> Tuple[str, Optional[str]]:
    if not check_in_str:
        return "absent", None
    if not check_out_str:
        if now_almaty.date() <= target_date:
            return "working", None
        return "worked_no_checkout", None

    worked_minutes = _worked_minutes(check_in_str, check_out_str)
    late = is_late(check_in_str)
    if worked_minutes >= FULL_DAY_MINUTES:
        return "worked", None
    deficit = _minutes_to_hours_str(FULL_DAY_MINUTES - worked_minutes)
    if late:
        return "late_and_underworked", deficit
    return "underworked", deficit


def calculate_today_summary(
    users: List[Dict[str, Any]],
    logs: List[Dict[str, Any]],
    schedule: List[Dict[str, Any]],
    holidays: List[Dict[str, Any]],
    target_date: date,
    now_almaty: datetime,
) -> Dict[str, Any]:
    holidays_map = _build_holidays_map(holidays)
    schedule_index = _build_schedule_index(schedule)
    user_day_logs = _build_user_day_logs(logs)
    date_str = target_date.strftime("%Y-%m-%d")
    rows = []
    for user in users:
        uid = str(user.get("user_id") or user.get("id") or "").strip()
        name = user.get("full_name") or user.get("name") or "—"
        if not uid:
            continue
        fn = str(name).strip()
        holiday_name = holidays_map.get(date_str)
        personal_off = (fn, date_str) in schedule_index
        is_day_off = _is_weekend(target_date) or bool(holiday_name) or personal_off
        if is_day_off:
            rows.append(
                {
                    "userId": uid,
                    "fullName": name,
                    "status": "day_off",
                    "checkIn": "",
                    "checkOut": "",
                    "hoursWorked": None,
                    "isLate": False,
                }
            )
            continue
        day_logs = user_day_logs.get((uid, date_str), [])
        cin, cout = _extract_check_times(day_logs)
        st, udef = _resolve_day_status(cin, cout, target_date, now_almaty)
        wh = "N/A" if st in ("worked_no_checkout", "working") else calculate_hours(cin, cout)
        rows.append(
            {
                "userId": uid,
                "fullName": name,
                "status": st,
                "checkIn": cin or "",
                "checkOut": cout or "",
                "hoursWorked": wh,
                "isLate": is_late(cin) if cin else False,
                "underworkedBy": udef,
            }
        )
    rows.sort(key=lambda r: str(r.get("fullName", "")))
    summary = {"total": 0, "present": 0, "absent": 0, "late": 0, "working": 0}
    for r in rows:
        if r.get("status") == "day_off":
            continue
        summary["total"] += 1
        if r.get("status") == "absent":
            summary["absent"] += 1
        else:
            summary["present"] += 1
        if r.get("isLate"):
            summary["late"] += 1
        if r.get("status") == "working":
            summary["working"] += 1
    return {"date": date_str, "employees": rows, "summary": summary}


def calculate_employee_month_analytics(
    users: List[Dict[str, Any]],
    logs: List[Dict[str, Any]],
    schedule: List[Dict[str, Any]],
    holidays: List[Dict[str, Any]],
    first_day: date,
    last_day: date,
    target_user_id: str,
    now_almaty: datetime,
) -> Dict[str, Any]:
    holidays_map = _build_holidays_map(holidays)
    user_day_logs = _build_user_day_logs(logs)
    selected_user = next(
        (u for u in users if str(u.get("user_id", "")).strip() == str(target_user_id).strip()),
        None,
    )
    if not selected_user:
        return {"error": "user_not_found"}

    full_name = str(selected_user.get("full_name") or selected_user.get("name") or "").strip()
    schedule_index = _build_schedule_index(
        [s for s in schedule if str(s.get("full_name", "")).strip() == full_name]
    )
    schedule_by_date = {
        str(item.get("date", ""))[:10]: item
        for item in schedule
        if str(item.get("full_name", "")).strip() == full_name
    }

    days: List[Dict[str, Any]] = []
    late_count = 0
    worked_days = 0
    absences = 0
    total_minutes = 0
    planned_days = 0

    current = first_day
    while current <= last_day:
        date_str = current.strftime("%Y-%m-%d")
        day_logs = user_day_logs.get((str(target_user_id).strip(), date_str), [])
        day_schedule = schedule_by_date.get(date_str)
        holiday_name = holidays_map.get(date_str)
        check_in, check_out = _extract_check_times(day_logs)
        personal_off = (full_name, date_str) in schedule_index
        is_day_off = _is_weekend(current) or bool(holiday_name) or personal_off
        is_plan_day = (
            not _is_weekend(current) and not holiday_name and not personal_off
        )
        has_marks = bool(check_in or check_out)
        status, underworked_by = _resolve_day_status(check_in, check_out, current, now_almaty)
        worked_hours = (
            "N/A"
            if status in ("worked_no_checkout", "working")
            else calculate_hours(check_in, check_out)
        )
        worked_minutes = 0 if worked_hours == "N/A" else _hours_to_minutes(worked_hours)
        if is_day_off:
            status = "day_off"
            worked_minutes = 0
        if is_plan_day:
            planned_days += 1
            if has_marks:
                worked_days += 1
                if check_in and is_late(check_in):
                    late_count += 1
            else:
                absences += 1
        total_minutes += worked_minutes
        days.append(
            {
                "date": date_str,
                "weekday": current.strftime("%a"),
                "status": status,
                "checkIn": check_in,
                "checkOut": check_out,
                "hoursWorked": worked_hours,
                "isLate": is_late(check_in),
                "underworkedBy": underworked_by,
                "scheduleType": day_schedule.get("type") if day_schedule else None,
                "scheduleNote": day_schedule.get("note") if day_schedule else None,
                "holidayName": holiday_name,
            }
        )
        current += timedelta(days=1)

    norm_minutes = planned_days * NORM_DAY_MINUTES
    completion = int(round((total_minutes / norm_minutes) * 100)) if norm_minutes else 0
    emp_out = dict(selected_user)
    emp_out["fullName"] = full_name
    emp_out["userId"] = str(target_user_id).strip()
    return {
        "employee": emp_out,
        "stats": {
            "lateCount": late_count,
            "workDays": worked_days,
            "workHours": _minutes_to_hours_str(total_minutes),
            "absences": absences,
            "planDays": planned_days,
            "normHours": _minutes_to_hours_str(norm_minutes),
            "completionPercent": completion,
        },
        "days": days,
    }


def calculate_timesheet(
    users: List[Dict[str, Any]],
    logs: List[Dict[str, Any]],
    schedule: List[Dict[str, Any]],
    holidays: List[Dict[str, Any]],
    employee_directory: Optional[List[Dict[str, Any]]],
    first_day: date,
    last_day: date,
) -> Dict[str, Any]:
    schedule_index = _build_schedule_index(schedule)
    holidays_map = _build_holidays_map(holidays)
    user_day_logs = _build_user_day_logs(logs)
    position_by_user_id: Dict[str, str] = {}
    position_by_full_name: Dict[str, str] = {}
    for item in employee_directory or []:
        uid = str(item.get("user_id", "")).strip()
        fn = str(item.get("full_name", "")).strip()
        pos = (
            item.get("position")
            or item.get("role")
            or item.get("job_title")
            or item.get("title")
            or ""
        )
        pos = str(pos).strip()
        if uid and pos:
            position_by_user_id[uid] = pos
        if fn and pos:
            position_by_full_name[fn] = pos

    rows_out: List[Dict[str, Any]] = []
    for user in users:
        user_id = str(user.get("user_id", "")).strip()
        full_name = user.get("full_name") or user.get("name") or "Без имени"
        position = position_by_user_id.get(user_id) or position_by_full_name.get(str(full_name).strip()) or "-"
        late_count = 0
        absences = 0
        worked_days = 0
        total_minutes = 0
        plan_days = 0
        current_date = first_day
        while current_date <= last_day:
            date_str = current_date.strftime("%Y-%m-%d")
            is_plan_day = (
                not _is_weekend(current_date)
                and date_str not in holidays_map
                and (str(full_name).strip(), date_str) not in schedule_index
            )
            check_in, check_out = _extract_check_times(user_day_logs.get((user_id, date_str), []))
            has_marks = bool(check_in or check_out)
            if is_plan_day:
                plan_days += 1
                if has_marks:
                    worked_days += 1
                    if check_in and is_late(check_in):
                        late_count += 1
                else:
                    absences += 1
            total_minutes += _worked_minutes(check_in, check_out)
            current_date += timedelta(days=1)
        avg_minutes = int(round(total_minutes / worked_days)) if worked_days else 0
        norm_minutes = plan_days * NORM_DAY_MINUTES
        completion = int(round((total_minutes / norm_minutes) * 100)) if norm_minutes else 0
        rows_out.append(
            {
                "userId": user_id,
                "fullName": full_name,
                "position": position,
                "workDaysFact": worked_days,
                "workDaysPlan": plan_days,
                "lateCount": late_count,
                "absences": absences,
                "totalHours": _minutes_to_hours_str(total_minutes),
                "avgHoursPerDay": _minutes_to_hours_str(avg_minutes),
                "completionPercent": completion,
            }
        )
    rows_out.sort(key=lambda row: str(row["fullName"]))
    return {"rows": rows_out}


def calculate_analytics(
    users: List[Dict[str, Any]],
    logs: List[Dict[str, Any]],
    schedule: List[Dict[str, Any]],
    holidays: List[Dict[str, Any]],
    first_day: date,
    last_day: date,
) -> Dict[str, Any]:
    """Сводка по компании за месяц + календарь по рабочим дням (как в прежней версии UI)."""
    holidays_map = _build_holidays_map(holidays)
    schedule_index = _build_schedule_index(schedule)
    user_day_logs = _build_user_day_logs(logs)
    user_ids = [str(u.get("user_id", "")).strip() for u in users if str(u.get("user_id", "")).strip()]

    plan_dates: List[date] = []
    cur = first_day
    while cur <= last_day:
        ds = cur.strftime("%Y-%m-%d")
        if not _is_weekend(cur) and ds not in holidays_map:
            plan_dates.append(cur)
        cur += timedelta(days=1)

    present_per_day: List[int] = []
    late_days_flags: List[bool] = []
    absence_days_flags: List[bool] = []
    calendar: Dict[str, Dict[str, Any]] = {}

    for d in plan_dates:
        ds = d.strftime("%Y-%m-%d")
        present = 0
        absent = 0
        late_cnt = 0
        late_any = False
        absent_any = False
        expected = 0
        for uid in user_ids:
            u = next((x for x in users if str(x.get("user_id", "")).strip() == uid), None)
            fn = str(u.get("full_name") or u.get("name") or "").strip() if u else ""
            if (fn, ds) in schedule_index:
                continue
            expected += 1
            cin, cout = _extract_check_times(user_day_logs.get((uid, ds), []))
            if cin:
                present += 1
                if is_late(cin):
                    late_cnt += 1
                    late_any = True
            else:
                absent += 1
                absent_any = True
        present_per_day.append(present)
        late_days_flags.append(late_any)
        absence_days_flags.append(absent_any)

        day_status = "good"
        if expected > 0:
            ratio = absent / expected
            if ratio == 0:
                day_status = "perfect"
            elif ratio < 0.2:
                day_status = "good"
            elif ratio < 0.5:
                day_status = "warning"
            else:
                day_status = "bad"
        calendar[ds] = {
            "present": present,
            "absent": absent,
            "late": late_cnt,
            "status": day_status,
        }

    plan_days = len(plan_dates)
    avg_att = (
        round(sum(present_per_day) / len(present_per_day), 1) if present_per_day else 0.0
    )
    days_with_late = sum(1 for x in late_days_flags if x)
    days_with_absence = sum(1 for x in absence_days_flags if x)

    return {
        "planDays": plan_days,
        "headcount": len(user_ids),
        "avgPresentPerDay": avg_att,
        "daysWithLate": days_with_late,
        "daysWithAbsence": days_with_absence,
        "calendar": calendar,
        "stats": {
            "totalDays": plan_days,
            "avgPresent": avg_att,
            "lateDays": days_with_late,
            "absentDays": days_with_absence,
        },
    }
