from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
from datetime import datetime
import sqlite3
import os
import random
import json
import queue
import threading

App = Flask(__name__)
CORS(App)

DB_FILE = 'travel_buddy.db'

# ── Live updates (Server-Sent Events) ─────────────────────────────
# Every connected browser tab registers a Queue here. broadcast() pushes
# an event onto every subscriber's queue; each client's /api/stream
# generator blocks on its own queue and yields events as they arrive.
_subscribers = []
_subscribers_lock = threading.Lock()


def broadcast(event_type, data):
    payload = json.dumps({'type': event_type, 'data': data})
    with _subscribers_lock:
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _subscribers.remove(q)


def current_stats():
    return {
        'totalUsers': len(array_users),
        'pendingRequests': request_queue.get_size(),
        'totalRoutes': len(location_graph.get_all()),
        'matchesMade': match_count,
    }



array_users = []
users_counter = 1



class ListNode:
    def __init__(self, data):
        self.data = data
        self.next = None

class LinkedList:
    def __init__(self):
        self.head = None

    def append(self, data):
        node = ListNode(data)
        if not self.head:
            self.head = node
        else:
            curr = self.head
            while curr.next:
                curr = curr.next
            curr.next = node        

    def find_by_destination(self, dest):
        results = []
        curr = self.head
        while curr:
            if dest.lower() in curr.data.get('destination', '').lower():
                results.append(curr.data)
            curr = curr.next         
        return results

    def find_by_style(self, style):
        results = []
        curr = self.head
        while curr:
            if curr.data.get('travelStyle', '') == style:
                results.append(curr.data)
            curr = curr.next
        return results

    def to_list(self):
        result = []
        curr = self.head
        while curr:
            result.append(curr.data)
            curr = curr.next
        return result

list_travelers = LinkedList()


class Graph:
    def __init__(self):
        self.adj = {}

    def add_vertex(self, city):
        if city not in self.adj:
            self.adj[city] = []

    def add_edge(self, city1, city2):
        self.add_vertex(city1)
        self.add_vertex(city2)
        if city2 not in self.adj[city1]:
            self.adj[city1].append(city2)
        if city1 not in self.adj[city2]:
            self.adj[city2].append(city1)

    def get_neighbors(self, city):
        return self.adj.get(city, [])

    def bfs(self, start):
        if start not in self.adj:
            return []
        visited = {start}
        queue   = [start]
        result  = []
        while queue:
            city = queue.pop(0)
            result.append(city)
            for neighbor in self.adj[city]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
        return result                 # Fixed: return is outside the while loop

    def get_all(self):
        return self.adj


location_graph = Graph()
DEFAULT_ROUTES = [
    ('Tokyo',     'Seoul'),
    ('Seoul',     'Beijing'),
    ('Bangkok',   'Tokyo'),
    ('Bangkok',   'Bali'),
    ('Bangkok',   'Singapore'),
    ('Bali',      'Sydney'),
    ('Sydney',    'Singapore'),
    ('Singapore', 'Mumbai'),
    ('Paris',     'London'),
    ('Paris',     'Rome'),
    ('Paris',     'Amsterdam'),
    ('London',    'Amsterdam'),
    ('London',    'NYC'),
    ('Rome',      'Cairo'),
    ('Cairo',     'Dubai'),
    ('Dubai',     'Mumbai'),
    ('NYC',       'Toronto'),
]
for a, b in DEFAULT_ROUTES:
    location_graph.add_edge(a, b)



class Queue:
    def __init__(self):
        self.data = []

    def is_empty(self):
        return len(self.data) == 0

    def enqueue(self, item):
        self.data.append(item)        

    def dequeue(self):
        if self.is_empty():
            return None
        return self.data.pop(0)      

    def peek(self):
        return self.data[0] if self.data else None

    def get_size(self):
        return len(self.data)

    def to_list(self):
        return list(self.data)

request_queue = Queue()
match_count   = 0



class Stack:
    def __init__(self):
        self.data = []

    def is_empty(self):
        return len(self.data) == 0

    def push(self, item):
        self.data.append(item)

    def pop(self):
        if self.is_empty():
            return None
        return self.data.pop()       

    def peek(self):
        return self.data[-1] if self.data else None

    def size(self):
        return len(self.data)

    def to_list(self):
        return list(self.data)

undo_stack = Stack()



def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            email       TEXT UNIQUE NOT NULL,
            phone       TEXT,
            age         TEXT,
            gender      TEXT,
            destination TEXT NOT NULL,
            travelStyle TEXT,
            travelDate  TEXT,
            endDate     TEXT,
            ecName      TEXT,
            ecPhone     TEXT,
            photo       TEXT,
            created_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS requests (
            id          INTEGER PRIMARY KEY,
            from_user   TEXT NOT NULL,
            to_user     TEXT NOT NULL,
            destination TEXT NOT NULL,
            status      TEXT DEFAULT 'pending',
            created_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS matches (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user   TEXT,
            to_user     TEXT,
            destination TEXT,
            matched_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS undo_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id  INTEGER,
            from_user   TEXT,
            to_user     TEXT,
            destination TEXT,
            undone_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS reports (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            reporter    TEXT,
            reported    TEXT,
            reason      TEXT,
            created_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS graph_edges (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            city1       TEXT NOT NULL,
            city2       TEXT NOT NULL,
            UNIQUE(city1, city2)
        );
    ''')
    conn.commit()
    conn.close()

def load_db_into_memory():
    """Load existing DB rows back into in-memory structures on startup."""
    global users_counter, match_count
    conn = get_db()
    c = conn.cursor()

    # Reload users into Array + Linked List
    for row in c.execute("SELECT * FROM users ORDER BY id"):
        user = dict(row)
        array_users.append(user)
        list_travelers.append(user)
        if user['id'] >= users_counter:
            users_counter = user['id'] + 1

    # Reload pending queue requests
    for row in c.execute("SELECT * FROM requests WHERE status='pending' ORDER BY id"):
        req = dict(row)
        request_queue.enqueue(req)
        undo_stack.push(req)

    # Reload match count
    row = c.execute("SELECT COUNT(*) as cnt FROM matches").fetchone()
    match_count = row['cnt'] if row else 0

    # Reload any custom routes added via /api/graph/edge
    for row in c.execute("SELECT city1, city2 FROM graph_edges"):
        location_graph.add_edge(row['city1'], row['city2'])

    conn.close()


SEED_USERS = [
    { 'name':'Arjun Mehta',  'email':'arjun@mail.com',  'phone':'+91 9876543210', 'age':'26', 'gender':'Male',   'destination':'Tokyo',  'travelStyle':'Adventure',    'travelDate':'2026-07-10', 'endDate':'', 'ecName':'', 'ecPhone':'', 'photo':'' },
    { 'name':'Priya Sharma', 'email':'priya@mail.com',  'phone':'+91 9123456789', 'age':'24', 'gender':'Female', 'destination':'Paris',  'travelStyle':'Cultural',     'travelDate':'2026-08-05', 'endDate':'', 'ecName':'', 'ecPhone':'', 'photo':'' },
    { 'name':'Rahul Nair',   'email':'rahul@mail.com',  'phone':'+91 9988776655', 'age':'29', 'gender':'Male',   'destination':'Bali',   'travelStyle':'Budget',       'travelDate':'2026-09-01', 'endDate':'', 'ecName':'', 'ecPhone':'', 'photo':'' },
    { 'name':'Sneha Iyer',   'email':'sneha@mail.com',  'phone':'+91 9871234560', 'age':'23', 'gender':'Female', 'destination':'Tokyo',  'travelStyle':'Cultural',     'travelDate':'2026-07-15', 'endDate':'', 'ecName':'', 'ecPhone':'', 'photo':'' },
    { 'name':'Vikram Das',   'email':'vikram@mail.com', 'phone':'+91 9000011111', 'age':'31', 'gender':'Male',   'destination':'London', 'travelStyle':'Luxury',       'travelDate':'2026-06-20', 'endDate':'', 'ecName':'', 'ecPhone':'', 'photo':'' },
    { 'name':'Meera Pillai', 'email':'meera@mail.com',  'phone':'+91 9222233333', 'age':'27', 'gender':'Female', 'destination':'Bali',   'travelStyle':'Adventure',    'travelDate':'2026-09-15', 'endDate':'', 'ecName':'', 'ecPhone':'', 'photo':'' },
    { 'name':'Aditya Kumar', 'email':'adi@mail.com',    'phone':'+91 9444455555', 'age':'25', 'gender':'Male',   'destination':'Paris',  'travelStyle':'Budget',       'travelDate':'2026-08-20', 'endDate':'', 'ecName':'', 'ecPhone':'', 'photo':'' },
    { 'name':'Divya Menon',  'email':'divya@mail.com',  'phone':'+91 9666677777', 'age':'22', 'gender':'Female', 'destination':'Dubai',  'travelStyle':'Luxury',       'travelDate':'2026-07-25', 'endDate':'', 'ecName':'', 'ecPhone':'', 'photo':'' },
]

def seed_if_empty():
    global users_counter
    conn = get_db()
    c = conn.cursor()
    count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        for u in SEED_USERS:
            c.execute('''
                INSERT OR IGNORE INTO users
                (name,email,phone,age,gender,destination,travelStyle,travelDate,endDate,ecName,ecPhone,photo,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (u['name'], u['email'], u['phone'], u['age'], u['gender'],
                  u['destination'], u['travelStyle'], u['travelDate'],
                  u['endDate'], u['ecName'], u['ecPhone'], u['photo'],
                  datetime.now().isoformat()))
        conn.commit()
    conn.close()



def user_to_dict(row):
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return row



@App.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'message': 'Travel Buddy Finder API running'})



@App.route('/api/users', methods=['POST'])
def register_user():
    global users_counter
    data = request.get_json() or {}

    name        = data.get('name', '').strip()
    email       = data.get('email', '').strip()
    destination = data.get('destination', '').strip()

    
    if not name or not email or not destination:
        return jsonify({'error': 'Name, email and destination are required.'}), 400

    
    if any(u.get('email') == email for u in array_users):
        return jsonify({'error': 'A profile with this email already exists.'}), 409

    user = {
        'id':          users_counter,
        'name':        name,
        'email':       email,
        'phone':       data.get('phone', '').strip(),
        'age':         data.get('age', ''),
        'gender':      data.get('gender', ''),
        'destination': destination,
        'travelStyle': data.get('travelStyle', ''),
        'travelDate':  data.get('travelDate', ''),
        'endDate':     data.get('endDate', ''),
        'ecName':      data.get('ecName', '').strip(),
        'ecPhone':     data.get('ecPhone', '').strip(),
        'photo':       data.get('photo', ''),       
        'created_at':  datetime.now().isoformat()
    }

    
    array_users.append(user)
    users_counter += 1

    
    list_travelers.append(user)

   
    city = destination.split(',')[0].strip()
    location_graph.add_vertex(city)

    conn = get_db()
    try:
        conn.execute('''
            INSERT INTO users
            (name,email,phone,age,gender,destination,travelStyle,travelDate,endDate,ecName,ecPhone,photo,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (user['name'], user['email'], user['phone'], user['age'], user['gender'],
              user['destination'], user['travelStyle'], user['travelDate'],
              user['endDate'], user['ecName'], user['ecPhone'], user['photo'],
              user['created_at']))
        conn.commit()
    finally:
        conn.close()

    broadcast('new_user', {'name': name, 'destination': destination, 'stats': current_stats()})

    return jsonify({'message': f'Profile registered successfully! Welcome, {name}.', 'user': user}), 201



def _parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, '%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def compute_compatibility(user_a, user_b):
    """Score two travelers 0-100 on how good a match they are.
    Combines: destination proximity (Graph/BFS), travel style, date overlap, age.
    """
    if user_a.get('id') == user_b.get('id'):
        return None

    score = 0
    breakdown = {}

    # Destination (0-40): exact match beats "reachable via graph"
    dest_a = (user_a.get('destination') or '').split(',')[0].strip().lower()
    dest_b = (user_b.get('destination') or '').split(',')[0].strip().lower()
    if dest_a and dest_a == dest_b:
        score += 40
        breakdown['destination'] = 40
    elif dest_a and dest_b:
        matched_city = next((k for k in location_graph.get_all() if k.lower() == dest_a), None)
        reachable = {c.lower() for c in location_graph.bfs(matched_city)} if matched_city else set()
        if dest_b in reachable:
            score += 18
            breakdown['destination'] = 18
        else:
            breakdown['destination'] = 0
    else:
        breakdown['destination'] = 0

    # Travel style (0-25)
    style_a = user_a.get('travelStyle') or ''
    style_b = user_b.get('travelStyle') or ''
    if style_a and style_a == style_b:
        score += 25
        breakdown['travelStyle'] = 25
    else:
        breakdown['travelStyle'] = 0

    # Date overlap (0-25)
    a_start, a_end = _parse_date(user_a.get('travelDate')), _parse_date(user_a.get('endDate'))
    b_start, b_end = _parse_date(user_b.get('travelDate')), _parse_date(user_b.get('endDate'))
    date_pts = 0
    if a_start and b_start:
        a_end_eff = a_end or a_start
        b_end_eff = b_end or b_start
        overlap = a_start <= b_end_eff and b_start <= a_end_eff
        if overlap:
            date_pts = 25
        else:
            gap_days = min(abs((a_start - b_start).days), abs((a_start - b_end_eff).days))
            if gap_days <= 14:
                date_pts = 12
    score += date_pts
    breakdown['dates'] = date_pts

    # Age proximity (0-10)
    age_pts = 0
    try:
        age_a, age_b = int(user_a.get('age')), int(user_b.get('age'))
        diff = abs(age_a - age_b)
        if diff <= 3:
            age_pts = 10
        elif diff <= 7:
            age_pts = 5
    except (TypeError, ValueError):
        pass
    score += age_pts
    breakdown['age'] = age_pts

    return {'score': min(100, score), 'breakdown': breakdown}


@App.route('/api/match/<int:uid>', methods=['GET'])
def get_matches(uid):
    user = next((u for u in array_users if u.get('id') == uid), None)
    if not user:
        return jsonify({'error': 'User not found.'}), 404

    limit = request.args.get('limit', default=10, type=int)
    matches = []
    for other in array_users:
        result = compute_compatibility(user, other)
        if result is None:
            continue
        matches.append({
            'user': {k: v for k, v in other.items() if k != 'photo'},
            'score': result['score'],
            'breakdown': result['breakdown']
        })

    matches.sort(key=lambda m: m['score'], reverse=True)
    return jsonify({'baseUser': uid, 'matches': matches[:limit]})


@App.route('/api/users', methods=['GET'])
def get_users():
    dest  = request.args.get('destination', '').strip()
    style = request.args.get('style', '').strip()

    if dest:
        # Linked List traversal
        results = list_travelers.find_by_destination(dest)
    else:
        # Array — full list
        results = list(array_users)

    if style:
        results = [u for u in results if u.get('travelStyle') == style]

   
    safe = [{k: v for k, v in u.items() if k != 'photo'} for u in results]
    return jsonify(safe)



@App.route('/api/users/<int:uid>', methods=['GET'])
def get_user(uid):
    user = next((u for u in array_users if u.get('id') == uid), None)
    if not user:
        return jsonify({'error': 'User not found.'}), 404
    return jsonify({k: v for k, v in user.items() if k != 'photo'})


@App.route('/api/graph/<city>', methods=['GET'])
def get_city(city):
    # Case-insensitive lookup
    matched = None
    for k in location_graph.get_all():
        if k.lower() == city.lower():
            matched = k
            break

    if not matched:
        return jsonify({'error': f'"{city}" is not in the travel graph.'}), 404

    neighbors   = location_graph.get_neighbors(matched)
    bfs_result  = location_graph.bfs(matched)
    # Exclude start city itself from reachable list (matches frontend bfsAll behaviour)
    reachable   = [c for c in bfs_result if c != matched]

    return jsonify({
        'city':        matched,
        'neighbors':   neighbors,
        'allReachable': reachable
    })


@App.route('/api/graph', methods=['GET'])
def get_graph():
    return jsonify(location_graph.get_all())


@App.route('/api/graph/edge', methods=['POST'])
def add_edge():
    data  = request.get_json() or {}
    city1 = data.get('city1', '').strip()
    city2 = data.get('city2', '').strip()
    if not city1 or not city2:
        return jsonify({'error': 'city1 and city2 are required.'}), 400
    location_graph.add_edge(city1, city2)
    # Persist new edge
    conn = get_db()
    conn.execute("INSERT OR IGNORE INTO graph_edges(city1,city2) VALUES (?,?)", (city1, city2))
    conn.commit()
    conn.close()
    broadcast('new_route', {'city1': city1, 'city2': city2, 'stats': current_stats()})
    return jsonify({'message': f'Route added: {city1} ↔ {city2}'})



@App.route('/api/requests', methods=['POST'])
def send_request():
    global match_count
    data = request.get_json() or {}

    from_user   = data.get('from', '').strip()
    to_user     = data.get('to', '').strip()
    destination = data.get('destination', '').strip()

    if not from_user or not to_user or not destination:
        return jsonify({'error': 'from, to and destination are required.'}), 400

    req = {
        'id':          int(datetime.now().timestamp() * 1000),
        'from':        from_user,
        'to':          to_user,
        'destination': destination,
        'status':      'pending',
        'created_at':  datetime.now().isoformat()
    }

   
    request_queue.enqueue(req)

    
    undo_stack.push(req)

    
    conn = get_db()
    conn.execute('''
        INSERT INTO requests (id, from_user, to_user, destination, status, created_at)
        VALUES (?,?,?,?,?,?)
    ''', (req['id'], req['from'], req['to'], req['destination'], req['status'], req['created_at']))
    conn.commit()
    conn.close()

    broadcast('new_request', {'request': req, 'queueSize': request_queue.get_size(), 'stats': current_stats()})

    return jsonify({
        'message':   f'Request queued! Queue size: {request_queue.get_size()}',
        'request':   req,
        'queueSize': request_queue.get_size()
    }), 201


@App.route('/api/requests', methods=['GET'])
def get_requests():
    return jsonify({
        'queue': request_queue.to_list(),
        'size':  request_queue.get_size()
    })



@App.route('/api/requests/process', methods=['POST'])
def process_request():
    global match_count
    if request_queue.is_empty():
        return jsonify({'error': 'Queue is empty — nothing to process.'}), 400

    # ─ Queue: dequeue ─
    processed = request_queue.dequeue()
    match_count += 1

    
    conn = get_db()
    conn.execute("UPDATE requests SET status='processed' WHERE id=?", (processed.get('id'),))
    conn.execute('''
        INSERT INTO matches (from_user, to_user, destination, matched_at)
        VALUES (?,?,?,?)
    ''', (processed.get('from'), processed.get('to'), processed.get('destination'), datetime.now().isoformat()))
    conn.commit()
    conn.close()

    broadcast('match_made', {'processed': processed, 'totalMatches': match_count, 'stats': current_stats()})

    return jsonify({
        'message':     f"Matched: {processed.get('from')} ↔ {processed.get('to')} for {processed.get('destination')}",
        'processed':   processed,
        'totalMatches': match_count
    })


@App.route('/api/requests/undo', methods=['POST'])
def undo_request():
    if undo_stack.is_empty():
        return jsonify({'error': 'Stack is empty — no requests to undo.'}), 400

    # ─ Stack: pop ─
    undone = undo_stack.pop()

    # Remove from Queue if still pending
    request_queue.data = [r for r in request_queue.data if r.get('id') != undone.get('id')]

    # ─ SQLite: mark undone + log ─
    conn = get_db()
    conn.execute("UPDATE requests SET status='undone' WHERE id=?", (undone.get('id'),))
    conn.execute('''
        INSERT INTO undo_log (request_id, from_user, to_user, destination, undone_at)
        VALUES (?,?,?,?,?)
    ''', (undone.get('id'), undone.get('from'), undone.get('to'), undone.get('destination'), datetime.now().isoformat()))
    conn.commit()
    conn.close()

    broadcast('request_undone', {'undone': undone, 'stats': current_stats()})

    return jsonify({
        'message': f"Undone: {undone.get('from')} → {undone.get('to')} ({undone.get('destination')})",
        'undone':  undone,
        'stackSize': undo_stack.size()
    })


@App.route('/api/stats', methods=['GET'])
def get_stats():
    return jsonify({
        'users':     len(array_users),
        'queue':     request_queue.get_size(),
        'matches':   match_count,
        'undoStack': undo_stack.size(),
        'graphNodes': len(location_graph.get_all())
    })


@App.route('/api/stream', methods=['GET'])
def stream():
    """Server-Sent Events endpoint. Each connected client gets its own
    Queue subscribed to broadcast(); events are pushed the moment a
    registration/request/match/route happens elsewhere in the app."""
    client_q = queue.Queue(maxsize=100)
    with _subscribers_lock:
        _subscribers.append(client_q)

    def event_stream():
        try:
            # Send an initial snapshot so the UI has numbers immediately
            yield f"data: {json.dumps({'type': 'snapshot', 'data': {'stats': current_stats()}})}\n\n"
            while True:
                try:
                    payload = client_q.get(timeout=15)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    yield ": keep-alive\n\n"
        except GeneratorExit:
            pass
        finally:
            with _subscribers_lock:
                if client_q in _subscribers:
                    _subscribers.remove(client_q)

    return Response(stream_with_context(event_stream()), mimetype='text/event-stream',
                     headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@App.route('/api/reports', methods=['POST'])
def submit_report():
    data     = request.get_json() or {}
    reporter = data.get('reporter', 'Anonymous').strip()
    reported = data.get('reported', '').strip()
    reason   = data.get('reason', '').strip()

    if not reported or not reason:
        return jsonify({'error': 'Reported user name and reason are required.'}), 400

    conn = get_db()
    conn.execute('''
        INSERT INTO reports (reporter, reported, reason, created_at)
        VALUES (?,?,?,?)
    ''', (reporter, reported, reason, datetime.now().isoformat()))
    conn.commit()
    conn.close()

    return jsonify({'message': f'Report submitted for "{reported}". Our team will review within 24 hours.'}), 201



_otp_store = {}   # { email_or_phone : otp_string }

@App.route('/api/otp/send', methods=['POST'])
def send_otp():
    data    = request.get_json() or {}
    channel = data.get('channel', '')     # 'email' or 'mobile'
    target  = data.get('target', '').strip()

    if not target:
        return jsonify({'error': 'Email or phone is required.'}), 400

    otp = str(random.randint(100000, 999999))
    _otp_store[target] = otp

    # In a real app you would send via SMTP / SMS gateway here.
    # For demo we return the OTP directly so the frontend can show it.
    return jsonify({
        'message': f'OTP sent to {target}',
        'otp':     otp      # Remove this line in production!
    })


@App.route('/api/otp/verify', methods=['POST'])
def verify_otp():
    data    = request.get_json() or {}
    target  = data.get('target', '').strip()
    entered = data.get('otp', '').strip()

    expected = _otp_store.get(target)
    if not expected:
        return jsonify({'error': 'No OTP found. Please request a new one.'}), 400

    if entered == expected:
        del _otp_store[target]    # Consume OTP — one-time use
        return jsonify({'message': 'OTP verified successfully.', 'verified': True})
    else:
        return jsonify({'error': 'Incorrect OTP. Try again.', 'verified': False}), 400


if __name__ == '__main__':
    init_db()
    seed_if_empty()
    load_db_into_memory()

    print("""
╔══════════════════════════════════════════════════╗
║       ✈  Travel Buddy Finder  — Backend         ║
║       Running at http://localhost:5000           ║
╠══════════════════════════════════════════════════╣
║  Data Structures:                               ║
║    1. Array       → User profile storage        ║
║    2. Linked List → Destination matching        ║
║    3. Graph + BFS → City connectivity           ║
║    4. Queue (FIFO)→ Buddy request handling      ║
║    5. Stack (LIFO)→ Undo last request           ║
╠══════════════════════════════════════════════════╣
║  Database : travel_buddy.db  (SQLite)           ║
╚══════════════════════════════════════════════════╝
    """)

    App.run(debug=True, port=5000, threaded=True)
