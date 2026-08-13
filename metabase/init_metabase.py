"""Provisiona o Metabase sem interacao manual: cria o usuario admin (setup
wizard via API) e a conexao com o Athena (camada Gold), ambos de forma
idempotente - seguro rodar de novo a cada `docker compose up`.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

MB_URL = os.environ.get("MB_URL", "http://metabase:3000").rstrip("/")
ADMIN_EMAIL = os.environ["METABASE_ADMIN_EMAIL"]
ADMIN_PASSWORD = os.environ["METABASE_ADMIN_PASSWORD"]
SITE_NAME = os.environ.get("METABASE_SITE_NAME", "Projeto Integrado WISDM")
DATABASE_NAME = "WISDM Gold (Athena)"


def request(method, path, body=None, session=None):
    url = f"{MB_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if session:
        req.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, raw.decode(errors="replace")


def wait_healthy(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _ = request("GET", "/api/health")
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(3)
    raise SystemExit("Metabase nao respondeu /api/health a tempo.")


def athena_details():
    session_token = os.environ.get("AWS_SESSION_TOKEN", "").strip()
    bucket = os.environ["S3_BUCKET_NAME"]
    results_prefix = os.environ.get("ATHENA_RESULTS_PREFIX", "athena-results").strip("/")
    details = {
        "region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "workgroup": os.environ.get("ATHENA_WORKGROUP", "primary"),
        "s3_staging_dir": f"s3://{bucket}/{results_prefix}/",
        "catalog": "AwsDataCatalog",
        "dbname": os.environ.get("GOLD_GLUE_DATABASE_NAME", "projeto_integrado_wisdm_gold"),
        "access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    }
    if session_token:
        # AWS Academy Learner Lab usa credenciais temporarias (precisa do
        # session token). Nome exato da propriedade nao confirmado contra o
        # driver JDBC da AWS usado pelo Metabase - se a sincronizacao falhar
        # por causa disso, ajustar aqui.
        details["additional-options"] = f"SessionToken={session_token};"
    return details


def existing_database_id(session):
    status, resp = request("GET", "/api/database", session=session)
    if status >= 300 or resp is None:
        return None
    rows = resp.get("data", resp) if isinstance(resp, dict) else resp
    for row in rows:
        if row["name"] == DATABASE_NAME:
            return row["id"]
    return None


def ensure_database(session):
    # As credenciais da AWS Academy Learner Lab sao temporarias e expiram
    # (Athena passa a rejeitar com "security token included in the request
    # is expired"). Por isso, mesmo que a conexao ja exista, atualizamos os
    # `details` a cada boot para refletir o AWS_SESSION_TOKEN atual do .env -
    # criar so na primeira vez deixaria o Metabase preso num token vencido.
    database_id = existing_database_id(session)
    if database_id is not None:
        status, resp = request(
            "PUT",
            f"/api/database/{database_id}",
            {"details": athena_details()},
            session=session,
        )
        if status >= 300:
            print(f"AVISO: nao consegui atualizar as credenciais da conexao Athena (status {status}): {resp}")
        else:
            print(f"Conexao '{DATABASE_NAME}' ja existia, credenciais atualizadas.")
        return
    status, resp = request(
        "POST",
        "/api/database",
        {"engine": "athena", "name": DATABASE_NAME, "details": athena_details()},
        session=session,
    )
    if status >= 300:
        print(f"AVISO: nao consegui criar a conexao Athena (status {status}): {resp}")
    else:
        print(f"Conexao '{DATABASE_NAME}' criada com sucesso.")


def create_admin():
    status, props = request("GET", "/api/session/properties")
    token = props.get("setup-token") if isinstance(props, dict) else None
    if not token:
        return None

    print("Primeiro boot: criando usuario admin...")
    status, resp = request(
        "POST",
        "/api/setup",
        {
            "token": token,
            "user": {
                "first_name": "Admin",
                "last_name": "Projeto",
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
            },
            "prefs": {"site_name": SITE_NAME, "allow_tracking": False},
        },
    )
    if status >= 300:
        raise SystemExit(f"Falha ao criar admin (status {status}): {resp}")
    print("Admin criado.")
    return resp["id"]


def login_existing_admin():
    print("Admin ja existe, autenticando...")
    status, resp = request(
        "POST", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    )
    if status >= 300:
        raise SystemExit(f"Falha ao autenticar admin existente (status {status}): {resp}")
    return resp["id"]


def main():
    wait_healthy()
    session = create_admin() or login_existing_admin()
    ensure_database(session)


if __name__ == "__main__":
    main()
