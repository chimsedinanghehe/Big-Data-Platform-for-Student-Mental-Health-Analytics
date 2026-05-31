#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

POSTGRES_CONTAINER="student-mental-health-postgres"
POSTGRES_DB="student_mental_health_app"
POSTGRES_USER="student_app"
POSTGRES_PASSWORD="student_app_password"
POSTGRES_PORT="5433"
DATABASE_URL_DEFAULT="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}"

cd "$PROJECT_ROOT"

die() {
    echo "ERROR: $*" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "$1 was not found on PATH."
}

python_bin() {
    if [[ -x "$PROJECT_ROOT/venv/bin/python" ]]; then
        echo "$PROJECT_ROOT/venv/bin/python"
    elif [[ -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
        echo "$PROJECT_ROOT/.venv/bin/python"
    else
        die "Missing virtualenv Python. Create it with: python3 -m venv venv && venv/bin/python -m pip install -r requirements.txt"
    fi
}

docker_compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose "$@"
    elif command -v docker-compose >/dev/null 2>&1; then
        docker-compose "$@"
    else
        die "Docker Compose was not found. Install the docker compose plugin or docker-compose."
    fi
}

run_postgres() {
    need_cmd docker

    docker_compose up -d postgres

    echo "Waiting for PostgreSQL to accept connections..."
    for _ in {1..30}; do
        if docker exec "$POSTGRES_CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
            echo "PostgreSQL is ready on localhost:${POSTGRES_PORT}."
            return 0
        fi

        sleep 2
    done

    die "PostgreSQL did not become ready within 60 seconds."
}

run_backend() {
    local python
    python="$(python_bin)"

    [[ -f "$PROJECT_ROOT/.env" ]] || die "Missing root .env: $PROJECT_ROOT/.env"
    [[ -f "$PROJECT_ROOT/backend/.env" ]] || die "Missing backend .env: $PROJECT_ROOT/backend/.env"

    if ! grep -Eq '^OPENAI_API_KEY=.+$' "$PROJECT_ROOT/backend/.env"; then
        echo "WARNING: OPENAI_API_KEY is empty or missing in backend/.env."
    fi

    if command -v curl >/dev/null 2>&1 && curl -fsS "http://127.0.0.1:8000/health" >/dev/null 2>&1; then
        echo "FastAPI backend is already running at http://127.0.0.1:8000"
        echo "Health check: http://127.0.0.1:8000/health"
        return 0
    fi

    if command -v ss >/dev/null 2>&1 && ss -ltn "sport = :8000" | grep -q LISTEN; then
        die "Port 8000 is already in use. Stop that process or change the backend port."
    fi

    export DATABASE_URL="${DATABASE_URL:-$DATABASE_URL_DEFAULT}"

    cd "$PROJECT_ROOT"
    echo "Starting FastAPI backend..."
    echo "Health check: http://127.0.0.1:8000/health"
    echo "User database: PostgreSQL on 127.0.0.1:${POSTGRES_PORT}"
    echo "Stop with Ctrl+C"

    "$python" -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
}

run_dashboard() {
    local python dashboard_root
    python="$(python_bin)"
    dashboard_root="$PROJECT_ROOT/MentalSchool_Dashboard"

    [[ -d "$dashboard_root" ]] || die "Missing dashboard folder: $dashboard_root"

    cd "$dashboard_root"
    export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
    export STREAMLIT_SERVER_HEADLESS=true

    echo "Starting Streamlit dashboard..."
    echo "Dashboard: http://127.0.0.1:8501"
    echo "Stop with Ctrl+C"

    "$python" -m streamlit run app.py \
        --server.address 127.0.0.1 \
        --server.port 8501 \
        --server.headless true \
        --server.showEmailPrompt false \
        --server.enableCORS false \
        --server.enableXsrfProtection false
}

run_frontend() {
    local frontend_dir
    frontend_dir="$PROJECT_ROOT/frontend"

    [[ -f "$frontend_dir/package.json" ]] || die "Missing frontend package.json: $frontend_dir/package.json"

    need_cmd node
    need_cmd npm

    cd "$frontend_dir"

    if [[ ! -d "$frontend_dir/node_modules" ]]; then
        echo "frontend/node_modules is missing. Installing frontend dependencies..."
        npm install
    fi

    echo "Starting frontend..."
    echo "Open: http://127.0.0.1:5173"
    echo "Stop with Ctrl+C"

    npm run dev -- --port 5173
}

open_terminal() {
    local title="$1"
    local target="$2"
    local root_q script_q

    root_q="$(printf "%q" "$PROJECT_ROOT")"
    script_q="$(printf "%q" "$SCRIPT_PATH")"

    if command -v gnome-terminal >/dev/null 2>&1; then
        gnome-terminal --title="$title" -- bash -lc "cd $root_q; bash $script_q $target; status=\$?; echo; echo \"Process exited with status \$status.\"; exec bash"
    elif command -v x-terminal-emulator >/dev/null 2>&1; then
        x-terminal-emulator -T "$title" -e bash -lc "cd $root_q; bash $script_q $target; status=\$?; echo; echo \"Process exited with status \$status.\"; exec bash"
    else
        die "No supported terminal emulator found. Install gnome-terminal or run: bash $SCRIPT_PATH backend/dashboard/frontend"
    fi
}

run_all() {
    run_postgres

    open_terminal "Student Mental Health Backend" backend
    open_terminal "Student Mental Health Dashboard" dashboard
    open_terminal "Student Mental Health Frontend" frontend

    echo "Started backend, dashboard, and frontend in separate windows."
    echo "Backend:   http://127.0.0.1:8000/health"
    echo "Dashboard: http://127.0.0.1:8501"
    echo "Frontend:  http://127.0.0.1:5173"
}

case "${1:-all}" in
    all)
        run_all
        ;;
    postgres)
        run_postgres
        ;;
    backend)
        run_backend
        ;;
    dashboard)
        run_dashboard
        ;;
    frontend)
        run_frontend
        ;;
    *)
        echo "Usage: $0 [all|postgres|backend|dashboard|frontend]" >&2
        exit 2
        ;;
esac
