import subprocess, json, os, calendar, threading, uuid, time
from datetime import date
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, request, send_from_directory, Response

app = Flask(__name__, static_folder="static", template_folder="static")

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
DSH             = os.path.expanduser("~/.local/bin/dsh")
PROFILE         = "prod"
DOMAIN_MAP_FILE = os.path.join(os.path.dirname(__file__), "domain_map.json")

_cache_lock  = threading.Lock()
_search_cache = {}
_detail_cache = {}
_report_cache = {}
_staff_cache  = {}
_jobs         = {}   # job_id -> {"status": "running"|"done"|"error", "result": ...}

# ─────────────────────────────────────────────────────────────────────────────
# Domain map
# ─────────────────────────────────────────────────────────────────────────────
def load_domain_map():
    if os.path.exists(DOMAIN_MAP_FILE):
        try:
            with open(DOMAIN_MAP_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            result = {}
            for k, v in raw.items():
                if isinstance(v, str):
                    result[k] = {"name": v, "is_game": True, "recent_camp": ""}
                else:
                    result[k] = v
            return result
        except Exception:
            pass
    return {}

def save_domain_map(dm):
    with open(DOMAIN_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(dm, f, ensure_ascii=False, indent=2)

DOMAIN_MAP = load_domain_map()

def get_company(domain):
    e = DOMAIN_MAP.get(domain)
    return (e.get("name") or domain) if e else (domain or "미분류")

def is_game_domain(domain):
    e = DOMAIN_MAP.get(domain)
    return e.get("is_game", False) if e else False

# ─────────────────────────────────────────────────────────────────────────────
# DSH helpers
# ─────────────────────────────────────────────────────────────────────────────
def run_dsh(*args):
    r = subprocess.run([DSH, *args, "--profile", PROFILE, "-j"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        return None, r.stderr.strip()
    try:
        return json.loads(r.stdout), None
    except Exception as e:
        return None, str(e)

def get_staff_map():
    known = {8454: "Ben Baek"}
    with _cache_lock:
        if _staff_cache:
            return dict(_staff_cache)
    data, err = run_dsh("account", "list", "--role", "staff",
                        "--page-size", "300", "--results-only")
    if err or not data:
        return known
    result = {r["id"]: r.get("name", str(r["id"])) for r in data}
    result.update(known)
    if result:
        with _cache_lock:
            _staff_cache.update(result)
    return result

def search_adgroups(keyword):
    kw = keyword.strip() if keyword.strip() else "UA"
    with _cache_lock:
        if kw in _search_cache:
            return _search_cache[kw], None
    data, err = run_dsh("adgroup", "list", "--search", kw, "--type", "action",
                        "--page-size", "200", "--results-only",
                        "--select", "id,name,sales_manager_id")
    if not err and data:
        with _cache_lock:
            _search_cache[kw] = data
    return data or [], err

def get_adgroup_detail(ag_id):
    with _cache_lock:
        if ag_id in _detail_cache:
            return _detail_cache[ag_id]
    data, _ = run_dsh("adgroup", "get", str(ag_id), "--results-only",
                      "--select", "id,name,agent_fee_rate,advertiser_domains,sales_manager_id,budget,budget_spent,budget_remaining,is_closed")
    if data:
        with _cache_lock:
            _detail_cache[ag_id] = data
    return data

def get_report(ag_id, start, end):
    key = (ag_id, start, end)
    with _cache_lock:
        if key in _report_cache:
            return _report_cache[key]
    data, err = run_dsh("adgroup", "report", str(ag_id), "--start-date", start, "--end-date", end)
    if err or not data:
        return []   # 에러는 캐시 안 함
    result = data.get("data", {}).get("reports", [])
    with _cache_lock:
        _report_cache[key] = result
    return result

# ─────────────────────────────────────────────────────────────────────────────
# API routes
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/domain_map", methods=["GET"])
def api_domain_map_get():
    return jsonify(DOMAIN_MAP)

@app.route("/api/domain_map", methods=["POST"])
def api_domain_map_save():
    dm = request.json
    save_domain_map(dm)
    DOMAIN_MAP.clear()
    DOMAIN_MAP.update(dm)
    _detail_cache.clear()
    _report_cache.clear()
    return jsonify({"ok": True})

@app.route("/api/refresh_cache", methods=["POST"])
def api_refresh_cache():
    _search_cache.clear(); _staff_cache.clear()
    _detail_cache.clear(); _report_cache.clear()
    return jsonify({"ok": True})

# ── 홈: 백그라운드 잡 ──
@app.route("/api/jobs/home", methods=["POST"])
def start_home_job():
    today = date.today()
    cur_year, cur_month = today.year, today.month
    last_day = calendar.monthrange(cur_year, cur_month)[1]
    s = f"{cur_year}-{cur_month:02d}-01"
    e = f"{cur_year}-{cur_month:02d}-{last_day}"
    if cur_month == 1:
        prev_year, prev_month = cur_year - 1, 12
    else:
        prev_year, prev_month = cur_year, cur_month - 1
    prev_last = calendar.monthrange(prev_year, prev_month)[1]
    same_day  = min(today.day, prev_last)
    ps = f"{prev_year}-{prev_month:02d}-01"
    pe = f"{prev_year}-{prev_month:02d}-{same_day:02d}"
    prev_full_e = f"{prev_year}-{prev_month:02d}-{prev_last:02d}"

    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {"status": "running", "result": None}

    def _run():
        try:
            camps, err = search_adgroups("UA")
            if err or not camps:
                _jobs[job_id] = {"status": "error", "result": str(err)}
                return
            staff_map = get_staff_map()

            def _fetch(c):
                ag_id = c["id"]
                ag = get_adgroup_detail(ag_id) or {}
                domains = ag.get("advertiser_domains") or []
                domain = domains[0] if domains else ""
                if not is_game_domain(domain):
                    return None
                cur_reports       = get_report(ag_id, s, e)
                prev_reports      = get_report(ag_id, ps, pe)
                prev_full_reports = get_report(ag_id, ps, prev_full_e)
                cur_cost       = sum(r.get("cost", 0) or 0 for r in cur_reports)
                prev_cost      = sum(r.get("cost", 0) or 0 for r in prev_reports)
                prev_full_cost = sum(r.get("cost", 0) or 0 for r in prev_full_reports)
                budget  = ag.get("budget") or 0
                spent   = ag.get("budget_spent") or 0
                sm_id   = ag.get("sales_manager_id") or c.get("sales_manager_id")
                sm_name = staff_map.get(sm_id, f"ID:{sm_id}") if sm_id else "미배정"
                cur_daily  = {r["date"][:10]: r.get("cost", 0) or 0 for r in cur_reports}
                prev_daily = {r["date"][:10]: r.get("cost", 0) or 0 for r in prev_reports}
                return dict(
                    name=c["name"], company=get_company(domain),
                    sales_manager=sm_name,
                    budget=budget, spent=spent,
                    cur_cost=cur_cost, prev_cost=prev_cost,
                    is_new=(prev_full_cost == 0),
                    is_closed=bool(ag.get("is_closed")),
                    cur_daily=cur_daily, prev_daily=prev_daily,
                )

            rows = []
            with ThreadPoolExecutor(max_workers=12) as ex:
                for r in as_completed([ex.submit(_fetch, c) for c in camps]):
                    v = r.result()
                    if v:
                        rows.append(v)

            rows.sort(key=lambda x: x["cur_cost"], reverse=True)

            # 일별 집계
            daily_cur  = defaultdict(int)
            daily_prev = defaultdict(int)
            for r in rows:
                for d, v in r.get("cur_daily", {}).items():
                    daily_cur[d]  += v
                for d, v in r.get("prev_daily", {}).items():
                    daily_prev[d] += v

            _jobs[job_id] = {
                "status": "done",
                "result": {
                    "rows": rows,
                    "daily_cur":  dict(daily_cur),
                    "daily_prev": dict(daily_prev),
                    "cur_month":  cur_month,
                    "prev_month": prev_month,
                    "cur_year":   cur_year,
                    "today":      str(today),
                }
            }
        except Exception as ex:
            _jobs[job_id] = {"status": "error", "result": str(ex)}

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"job_id": job_id})

@app.route("/api/jobs/<job_id>", methods=["GET"])
def get_job(job_id):
    j = _jobs.get(job_id)
    if not j:
        return jsonify({"status": "not_found"}), 404
    return jsonify(j)

# ── 정산 ──
@app.route("/api/settlement", methods=["POST"])
def api_settlement():
    body = request.json
    kw    = body.get("keyword", "UA") or "UA"
    year  = int(body.get("year",  date.today().year))
    month = int(body.get("month", date.today().month))
    last_day = calendar.monthrange(year, month)[1]
    s = f"{year}-{month:02d}-01"
    e = f"{year}-{month:02d}-{last_day}"

    camps, err = search_adgroups(kw)
    if err or not camps:
        return jsonify({"error": str(err)}), 400

    staff_map = get_staff_map()

    def _fetch(c):
        ag_id   = c["id"]
        reports = get_report(ag_id, s, e)
        cost    = sum(r.get("cost", 0) or 0 for r in reports)
        if cost <= 0:
            return None
        ag      = get_adgroup_detail(ag_id) or {}
        domains = ag.get("advertiser_domains") or []
        domain  = domains[0] if domains else ""
        if not is_game_domain(domain):
            return None
        sm_id   = ag.get("sales_manager_id") or c.get("sales_manager_id")
        sm_name = staff_map.get(sm_id, f"ID:{sm_id}") if sm_id else "미배정"
        conv    = sum(r.get("conversion", 0) or 0 for r in reports)
        return dict(
            id=ag_id, name=c["name"],
            domain=domain, company=get_company(domain),
            sales_manager=sm_name,
            fee_rate=ag.get("agent_fee_rate") or 0,
            cost=cost, conv=conv,
        )

    results = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        for r in as_completed([ex.submit(_fetch, c) for c in camps]):
            v = r.result()
            if v:
                results.append(v)
    results.sort(key=lambda x: x["name"])
    return jsonify({"results": results, "period": f"{year}년 {month}월"})

# ── 소진 현황 ──
@app.route("/api/spend", methods=["POST"])
def api_spend():
    body  = request.json
    kw    = body.get("keyword", "UA") or "UA"
    start = body.get("start")
    end   = body.get("end")
    selected = body.get("selected", [])

    camps, err = search_adgroups(kw)
    if err:
        return jsonify({"error": str(err)}), 400
    if selected:
        camps = [c for c in camps if c["name"] in selected]

    id_map = {c["name"]: c["id"] for c in camps}
    rows = []
    for name, ag_id in id_map.items():
        for r in get_report(ag_id, start, end):
            rows.append({"campaign": name, "date": r["date"][:10],
                         "cost": r.get("cost", 0) or 0})
    return jsonify({"rows": rows, "camps": [c["name"] for c in camps]})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
