"""Monta o dashboard do Metabase exigido pela secao 4.7 do PDF (indicadores
principais, dados tratados, resultados do modelo, filtro, explicacao),
usando as 4 tabelas Gold ja sincronizadas via Athena. Idempotente: se rodado
de novo, atualiza os cards existentes em vez de duplicar.

Uso: python3 metabase/build_dashboard.py
Requer: METABASE_ADMIN_EMAIL / METABASE_ADMIN_PASSWORD no ambiente (mesmos
do .env), e o Metabase ja rodando com a conexao "WISDM Gold (Athena)" sincronizada.
"""

import json
import os
import urllib.error
import urllib.request
import uuid

MB_URL = os.environ.get("MB_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.environ["METABASE_ADMIN_EMAIL"]
ADMIN_PASSWORD = os.environ["METABASE_ADMIN_PASSWORD"]

DASHBOARD_NAME = "WISDM - Reconhecimento de Atividade Humana"
COLLECTION_NAME = None  # usa a colecao raiz


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


def login():
    status, resp = request("POST", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    if status >= 300:
        raise SystemExit(f"Login falhou ({status}): {resp}")
    return resp["id"]


def get_database_id(session, name="WISDM Gold (Athena)"):
    status, resp = request("GET", "/api/database", session=session)
    rows = resp.get("data", resp) if isinstance(resp, dict) else resp
    matches = [r["id"] for r in rows if r["name"] == name]
    if not matches:
        raise SystemExit(f"Database '{name}' nao encontrado no Metabase.")
    return matches[0]


def get_tables(session, database_id):
    status, resp = request("GET", f"/api/database/{database_id}?include=tables", session=session)
    tables = {}
    for t in resp["tables"]:
        status, meta = request("GET", f"/api/table/{t['id']}/query_metadata", session=session)
        fields = {f["name"]: f["id"] for f in meta["fields"]}
        tables[t["name"]] = {"id": t["id"], "fields": fields}
    return tables


def find_card_by_name(session, name):
    status, resp = request("GET", "/api/card", session=session)
    for c in resp:
        if c["name"] == name:
            return c["id"]
    return None


def upsert_card(session, name, dataset_query, display, viz_settings=None):
    body = {
        "name": name,
        "dataset_query": dataset_query,
        "display": display,
        "visualization_settings": viz_settings or {},
    }
    existing_id = find_card_by_name(session, name)
    if existing_id:
        status, resp = request("PUT", f"/api/card/{existing_id}", body, session=session)
        if status >= 300:
            raise SystemExit(f"Falha ao atualizar card '{name}' ({status}): {resp}")
        return existing_id
    status, resp = request("POST", "/api/card", body, session=session)
    if status >= 300:
        raise SystemExit(f"Falha ao criar card '{name}' ({status}): {resp}")
    return resp["id"]


def mbql(database_id, table_id, **query):
    return {
        "type": "query",
        "database": database_id,
        "query": {"source-table": table_id, **query},
    }


def field(field_id):
    return ["field", field_id, None]


def main():
    session = login()
    db_id = get_database_id(session)
    tables = get_tables(session, db_id)

    benchmark = tables["gold_activity_benchmark"]
    subject_summary = tables["gold_subject_activity_summary"]
    report = tables["ml_model_classification_report"]
    comparison = tables["ml_model_comparison"]

    print("Criando/atualizando cards...")

    card_total_subjects = upsert_card(
        session, "Indicador - Total de sujeitos",
        mbql(db_id, subject_summary["id"], aggregation=[["distinct", field(subject_summary["fields"]["subject_id"])]]),
        "scalar",
    )

    card_total_windows = upsert_card(
        session, "Indicador - Total de janelas processadas",
        mbql(db_id, benchmark["id"], aggregation=[["sum", field(benchmark["fields"]["n_windows"])]]),
        "scalar",
    )

    card_accuracy = upsert_card(
        session, "Indicador - Acuracia do modelo (MLP)",
        mbql(
            db_id, comparison["id"],
            fields=[field(comparison["fields"]["accuracy"])],
            filter=["=", field(comparison["fields"]["modelo"]), "mlp_hardcode"],
        ),
        "scalar",
    )

    card_windows_by_activity = upsert_card(
        session, "Dados tratados - Janelas por atividade",
        mbql(
            db_id, benchmark["id"],
            fields=[field(benchmark["fields"]["activity_label"]), field(benchmark["fields"]["n_windows"])],
            order_by=[["desc", field(benchmark["fields"]["n_windows"])]],
        ),
        "bar",
        {"graph.dimensions": ["activity_label"], "graph.metrics": ["n_windows"]},
    )

    card_subject_table = upsert_card(
        session, "Dados tratados - Resumo por sujeito e atividade",
        mbql(
            db_id, subject_summary["id"],
            fields=[
                field(subject_summary["fields"]["subject_id"]),
                field(subject_summary["fields"]["activity_label"]),
                field(subject_summary["fields"]["n_windows"]),
                field(subject_summary["fields"]["share_of_subject_windows"]),
                field(subject_summary["fields"]["avg_label_purity"]),
            ],
        ),
        "table",
    )

    card_model_comparison = upsert_card(
        session, "Resultados do modelo - Comparacao (baseline vs MLP)",
        mbql(
            db_id, comparison["id"],
            fields=[field(comparison["fields"]["modelo"]), field(comparison["fields"]["accuracy"])],
        ),
        "bar",
        {"graph.dimensions": ["modelo"], "graph.metrics": ["accuracy"]},
    )

    card_f1_by_activity = upsert_card(
        session, "Resultados do modelo - F1-score por atividade",
        mbql(
            db_id, report["id"],
            fields=[
                field(report["fields"]["activity_label"]),
                field(report["fields"]["activity_name"]),
                field(report["fields"]["f1_score"]),
            ],
            order_by=[["asc", field(report["fields"]["f1_score"])]],
        ),
        "bar",
        {"graph.dimensions": ["activity_name"], "graph.metrics": ["f1_score"]},
    )

    print("Criando/atualizando dashboard...")
    status, dashboards = request("GET", "/api/dashboard", session=session)
    existing = [d["id"] for d in dashboards if d["name"] == DASHBOARD_NAME]
    if existing:
        dashboard_id = existing[0]
    else:
        status, resp = request("POST", "/api/dashboard", {"name": DASHBOARD_NAME}, session=session)
        if status >= 300:
            raise SystemExit(f"Falha ao criar dashboard ({status}): {resp}")
        dashboard_id = resp["id"]

    activity_filter_id = uuid.uuid4().hex[:8]
    explanation_text = (
        "## Como este dashboard apoia a decisao\n\n"
        "Este painel resume o pipeline WISDM de reconhecimento de atividade "
        "humana via smartwatch (acelerometro + giroscopio). Os indicadores "
        "e graficos de **dados tratados** mostram a cobertura e distribuicao "
        "das janelas processadas na camada Silver/Gold; os graficos de "
        "**resultados do modelo** mostram o quanto o MLP treinado supera um "
        "baseline ingênuo e onde ele erra mais (F1 por atividade). O filtro "
        "de atividade permite investigar uma atividade especifica - por "
        "exemplo, confirmar que 'eating_sandwich' (L) tem o pior F1-score "
        "(0.18) por causa da confusao entre atividades de comer/beber, uma "
        "limitacao de sensoriamento (pulso) discutida no notebook de ML - "
        "informacao que apoia a decisao de nao usar o modelo para distinguir "
        "automaticamente entre tipos de refeicao sem sensores adicionais."
    )

    cards_payload = [
        {"id": -1, "card_id": card_total_subjects, "row": 0, "col": 0, "size_x": 6, "size_y": 4, "parameter_mappings": []},
        {"id": -2, "card_id": card_total_windows, "row": 0, "col": 6, "size_x": 6, "size_y": 4, "parameter_mappings": []},
        {"id": -3, "card_id": card_accuracy, "row": 0, "col": 12, "size_x": 6, "size_y": 4, "parameter_mappings": []},
        {"id": -4, "card_id": card_windows_by_activity, "row": 4, "col": 0, "size_x": 9, "size_y": 8,
         "parameter_mappings": [{
             "parameter_id": activity_filter_id, "card_id": card_windows_by_activity,
             "target": ["dimension", field(benchmark["fields"]["activity_label"])],
         }]},
        {"id": -5, "card_id": card_model_comparison, "row": 4, "col": 9, "size_x": 9, "size_y": 8, "parameter_mappings": []},
        {"id": -6, "card_id": card_f1_by_activity, "row": 12, "col": 0, "size_x": 18, "size_y": 8,
         "parameter_mappings": [{
             "parameter_id": activity_filter_id, "card_id": card_f1_by_activity,
             "target": ["dimension", field(report["fields"]["activity_label"])],
         }]},
        {"id": -7, "card_id": card_subject_table, "row": 20, "col": 0, "size_x": 18, "size_y": 8,
         "parameter_mappings": [{
             "parameter_id": activity_filter_id, "card_id": card_subject_table,
             "target": ["dimension", field(subject_summary["fields"]["activity_label"])],
         }]},
        {"id": -8, "card_id": None, "row": 28, "col": 0, "size_x": 18, "size_y": 6,
         "parameter_mappings": [],
         "visualization_settings": {"virtual_card": {"display": "text"}, "text": explanation_text}},
    ]

    status, resp = request(
        "PUT", f"/api/dashboard/{dashboard_id}/cards",
        {"cards": cards_payload}, session=session,
    )
    if status >= 300:
        raise SystemExit(f"Falha ao salvar cards do dashboard ({status}): {resp}")
    print("Cards adicionados ao dashboard.")

    status, resp = request(
        "PUT", f"/api/dashboard/{dashboard_id}",
        {
            "parameters": [{
                "id": activity_filter_id,
                "name": "Atividade",
                "slug": "activity_label",
                "type": "string/=",
            }]
        },
        session=session,
    )
    if status >= 300:
        raise SystemExit(f"Falha ao adicionar filtro ({status}): {resp}")
    print("Filtro 'Atividade' adicionado.")

    print(f"\nDashboard pronto: {MB_URL}/dashboard/{dashboard_id}")


if __name__ == "__main__":
    main()
