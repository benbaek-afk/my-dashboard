import json, os, calendar, threading, uuid, time, requests
from datetime import date
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="static", template_folder="static")

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
BASE_URL       = "https://dash-api-gateway.eks.buzzvil.com"
DOMAIN_MAP_FILE = os.path.join(os.path.dirname(__file__), "domain_map.json")

USERNAME = os.environ.get("DSH_USERNAME", "ben.baek@buzzvil.com")
PASSWORD = os.environ.get("DSH_PASSWORD", "")

_cache_lock   = threading.Lock()
_search_cache = {}
_detail_cache = {}
_report_cache = {}
_staff_cache  = {}
_jobs         = {}
_session_cookie = {"value": None}  # connect.sid 쿠키

# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────
def get_cookie():
    """유효한 세션 쿠키 반환."""
    with _cache_lock:
        if _session_cookie["value"]:
            return _session_cookie["value"]
    # 1) 환경변수 SESSION_COOKIE 우선 (Render 배포용)
    env_cookie = os.environ.get("SESSION_COOKIE", "").strip()
    if env_cookie:
        with _cache_lock:
            _session_cookie["value"] = env_cookie
        return env_cookie
    # 2) 로컬 session.yaml
    import re
    session_paths = [
        os.path.expanduser("~/.config/dsh/session.yaml"),
        os.path.expanduser("~/Library/Application Support/dsh/session.yaml"),
    ]
    for p in session_paths:
        if os.path.exists(p):
            try:
                with open(p) as f:
                    content = f.read()
                m = re.search(r'prod:\s*\n\s*cookie:\s*(\S+)', content)
                if m:
                    cookie = m.group(1).strip()
                    with _cache_lock:
                        _session_cookie["value"] = cookie
                    return cookie
            except Exception:
                pass
    return do_login()

def do_login():
    """BuzzVil API 로그인 → 새 쿠키 반환."""
    try:
        r = requests.post(f"{BASE_URL}/user/login",
                          json={"username": USERNAME, "password": PASSWORD},
                          timeout=10)
        for c in r.cookies:
            if c.name == "connect.sid":
                val = c.value
                with _cache_lock:
                    _session_cookie["value"] = val
                return val
    except Exception as e:
        print(f"Login error: {e}")
    return None

def api_get(path, params=None, retry=True):
    """GET 요청. 401이면 재로그인 후 1회 재시도."""
    cookie = get_cookie()
    if not cookie:
        return None, "No session"
    try:
        r = requests.get(f"{BASE_URL}{path}",
                         params=params,
                         cookies={"connect.sid": cookie},
                         timeout=30)
        if r.status_code == 401 and retry:
            with _cache_lock:
                _session_cookie["value"] = None
            do_login()
            return api_get(path, params, retry=False)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        return r.json(), None
    except Exception as e:
        return None, str(e)

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
# API helpers
# ─────────────────────────────────────────────────────────────────────────────
def get_staff_map():
    known = {8454: "Ben Baek"}
    with _cache_lock:
        if _staff_cache:
            return dict(_staff_cache)
    data, err = api_get("/ba/accounts", {"role": "staff", "page_size": 300})
    if err or not data:
        return known
    results = data.get("results", data) if isinstance(data, dict) else data
    result = {r["id"]: r.get("name", str(r["id"])) for r in results if isinstance(r, dict)}
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
    all_results = []
    page = 1
    while len(all_results) < 500:
        data, err = api_get("/ba/adgroups", {
            "search": kw, "type": "action",
            "page_size": 200, "page": page,
            "fields": "id,name,sales_manager_id"
        })
        if err or not data:
            return [], err
        results = data.get("results", [])
        all_results.extend(results)
        if not data.get("next") or len(results) < 200:
            break
        page += 1
    if all_results:
        with _cache_lock:
            _search_cache[kw] = all_results
    return all_results, None

def get_adgroup_detail(ag_id):
    with _cache_lock:
        if ag_id in _detail_cache:
            return _detail_cache[ag_id]
    data, _ = api_get(f"/ba/adgroups/{ag_id}", {
        "fields": "id,name,agent_fee_rate,advertiser_domains,sales_manager_id,budget_sum,budget_spent,budget_remaining,is_closed"
    })
    if data:
        # budget_sum → budget 필드로 정규화
        data["budget"] = data.get("budget_sum") or data.get("budget") or 0
        with _cache_lock:
            _detail_cache[ag_id] = data
    return data

def get_report(ag_id, start, end):
    key = (ag_id, start, end)
    with _cache_lock:
        if key in _report_cache:
            return _report_cache[key]
    data, err = api_get(f"/ba/adgroups/{ag_id}/reports", {
        "start_date": start, "end_date": end
    })
    if err or not data:
        return []
    result = data.get("reports", [])
    if result:
        with _cache_lock:
            _report_cache[key] = result
    return result

# ─────────────────────────────────────────────────────────────────────────────
# Flask routes
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
    with _cache_lock:
        _session_cookie["value"] = None
    return jsonify({"ok": True})

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

@app.route("/api/settlement", methods=["POST"])
def api_settlement():
    body  = request.json
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

@app.route("/api/spend", methods=["POST"])
def api_spend():
    body     = request.json
    kw       = body.get("keyword", "UA") or "UA"
    start    = body.get("start")
    end      = body.get("end")
    selected = body.get("selected", [])

    camps, err = search_adgroups(kw)
    if err:
        return jsonify({"error": str(err)}), 400
    if selected:
        camps = [c for c in camps if c["name"] in selected]

    rows = []
    for c in camps:
        for r in get_report(c["id"], start, end):
            rows.append({"campaign": c["name"], "date": r["date"][:10],
                         "cost": r.get("cost", 0) or 0})
    return jsonify({"rows": rows, "camps": [c["name"] for c in camps]})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
