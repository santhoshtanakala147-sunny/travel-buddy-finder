# Travel Buddy Finder ✈️

A demo web app for finding compatible travel companions, built as a Data Structures &
Algorithms course project. Frontend is a single-page HTML/CSS/JS app; backend is Flask
+ SQLite.

## Data structures used
| Structure         | Where it's used                                  |
|--------------------|---------------------------------------------------|
| Array              | In-memory user profile storage (`array_users`)     |
| Linked list         | Destination-based traveler lookup                 |
| Graph + BFS         | City connectivity / route discovery                |
| Queue (FIFO)        | Incoming buddy requests                            |
| Stack (LIFO)         | Undo-last-request                                  |

## Features
- Profile registration with (simulated) email + mobile OTP verification and photo capture
- Traveler search with a real compatibility-scoring algorithm — destination match
  (exact or graph-reachable), travel-style overlap, date-range overlap, and age
  proximity — served from `GET /api/match/<user_id>`
- Buddy request → approve/decline flow backed by the Queue/Stack endpoints
- City/route graph explorer (BFS) and safety reporting

## Running locally

```bash
pip install -r requirements.txt
python backend.py
```

The backend serves the API at `http://localhost:5000/api`. Then open `index.html`
directly in a browser (or serve it with any static file server).

## Project structure
```
.
├── index.html        # frontend (single-page app)
├── backend.py         # Flask API + SQLite persistence
└── requirements.txt
```
