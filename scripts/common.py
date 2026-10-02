# 토스증권 Open API 실측용 공용 코드. 표준 라이브러리만 쓴다(새 의존성 금지). 웹소켓은 websockets 가 있는 컨테이너에서 돈다.
import json, os, time, urllib.request, urllib.parse, urllib.error

BASE = 'https://openapi.tossinvest.com'
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOKEN_PATH = os.path.join(ROOT, 'data', 'token.json')


def load_env():
    env = {}
    p = os.path.join(ROOT, '.env')
    for ln in open(p):
        ln = ln.strip()
        if ln and not ln.startswith('#') and '=' in ln:
            k, v = ln.split('=', 1); env[k.strip()] = v.strip()
    return env


def get_token(force=False):
    """Client Credentials 토큰. 만료 60초 전까지 재사용(AUTH 그룹 초당 5회, 새 발급이 옛 토큰을 무효화)."""
    if not force and os.path.exists(TOKEN_PATH):
        t = json.load(open(TOKEN_PATH))
        if t.get('expires_at', 0) - 60 > time.time():
            return t['access_token']
    env = load_env()
    body = urllib.parse.urlencode({'grant_type': 'client_credentials', 'client_id': env['TOSS_CLIENT_ID'], 'client_secret': env['TOSS_CLIENT_SECRET']}).encode()
    req = urllib.request.Request(BASE + '/oauth2/token', data=body, headers={'Content-Type': 'application/x-www-form-urlencoded'})
    with urllib.request.urlopen(req, timeout=15) as r:
        t = json.load(r)
    t['expires_at'] = time.time() + int(t.get('expires_in', 3600))
    os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
    with open(TOKEN_PATH, 'w') as f:
        json.dump(t, f); os.chmod(TOKEN_PATH, 0o600)
    return t['access_token']


def get(path, params=None, token=None):
    """GET 한 번. 429 면 Retry-After 만큼 쉬고 1회 재시도. 응답 헤더의 rate limit 값을 같이 돌려준다."""
    token = token or get_token()
    url = BASE + path + ('?' + urllib.parse.urlencode(params) if params else '')
    req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'})
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                h = {k: r.headers.get(k) for k in ('X-RateLimit-Limit', 'X-RateLimit-Remaining', 'X-RateLimit-Reset')}
                return json.load(r), h
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt == 0:
                time.sleep(float(e.headers.get('Retry-After', '1'))); continue
            raise RuntimeError(f'{e.code} {path}: {e.read()[:300]!r}')
