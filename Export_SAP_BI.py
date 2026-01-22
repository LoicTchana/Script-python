import requests
import datetime
import os
import subprocess
import logging
from pathlib import Path

# =========================================================
# CONFIGURATION GÉNÉRALE
# =========================================================

SAP_BI_URL = "https://sapbi.server.local:6405/biprws"   # 🔁 À ADAPTER
AUTH_TYPE = "secEnterprise"
USERNAME = "SAP_USER"
PASSWORD = "SAP_PASSWORD"

# Documents WebI
WEBI_PAYSAGE_EMPTY_ID = "123456"
WEBI_HISTO_TRX_ID     = "789012"

# Dossiers de sortie
OUTPUT_PAYSAGE = Path(r"C:\Users\USER\AFRILAND FIRSTBANK\Departement DRI-General\Donnees TPE")
OUTPUT_HISTO   = Path(r"C:\Users\emmanuel_teinga\AFRILAND FIRSTBANK\Departement Intelligence Artificielle DRI -General\Donnees TPE\Historiques")

# Script métier à lancer après export
SCRIPT_METIER = Path(r"C:\Scripts\point_situation_tpe_auto.py")

# Log
LOG_FILE = Path(r"C:\Scripts\logs\export_sap_bi.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# =========================================================
# CALCUL DES DATES MÉTIER
# =========================================================

def calcul_dates():
    today = datetime.date.today()
    date_fin = today - datetime.timedelta(days=1)

    # vendredi de la semaine précédente
    date_debut = date_fin - datetime.timedelta(days=(date_fin.weekday() - 4) % 7 + 7)

    return date_debut.strftime("%Y-%m-%d"), date_fin.strftime("%Y-%m-%d")

# =========================================================
# CONNEXION SAP BI
# =========================================================

def login():
    logging.info("Connexion à SAP BI...")
    payload = f"""<attrs>
        <attr name="userName">{USERNAME}</attr>
        <attr name="password">{PASSWORD}</attr>
        <attr name="auth">{AUTH_TYPE}</attr>
    </attrs>"""

    r = requests.post(
        f"{SAP_BI_URL}/logon/long",
        data=payload,
        headers={"Content-Type": "application/xml"},
        verify=False
    )
    r.raise_for_status()
    return r.headers["X-SAP-LogonToken"]

# =========================================================
# EXPORT WEBI
# =========================================================

def export_webi(token, doc_id, prompts, output_file):
    logging.info(f"Export WebI {doc_id}")

    headers = {
        "X-SAP-LogonToken": token,
        "Content-Type": "application/xml"
    }

    # Création instance
    r = requests.post(
        f"{SAP_BI_URL}/documents/{doc_id}/instances",
        headers=headers,
        verify=False
    )
    r.raise_for_status()
    instance_id = r.headers["Location"].split("/")[-1]

    # Injection des prompts
    prompt_xml = "<prompts>"
    for k, v in prompts.items():
        prompt_xml += f"""
        <prompt>
            <id>{k}</id>
            <answer><values><value>{v}</value></values></answer>
        </prompt>
        """
    prompt_xml += "</prompts>"

    requests.put(
        f"{SAP_BI_URL}/documents/{doc_id}/instances/{instance_id}/parameters",
        data=prompt_xml,
        headers=headers,
        verify=False
    )

    # Export CSV
    r = requests.get(
        f"{SAP_BI_URL}/documents/{doc_id}/instances/{instance_id}/export?format=csv",
        headers=headers,
        verify=False
    )
    r.raise_for_status()

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "wb") as f:
        f.write(r.content)

    logging.info(f"Fichier exporté : {output_file}")

    # Suppression instance
    requests.delete(
        f"{SAP_BI_URL}/documents/{doc_id}/instances/{instance_id}",
        headers=headers,
        verify=False
    )

# =========================================================
# MAIN
# =========================================================

def main():
    try:
        date_debut, date_fin = calcul_dates()
        logging.info(f"Période : {date_debut} → {date_fin}")

        token = login()

        # -------- PAYSAGE EMPTY --------
        export_webi(
            token,
            WEBI_PAYSAGE_EMPTY_ID,
            {},
            OUTPUT_PAYSAGE / "Gestionnaire_AFB.csv"
        )

        # -------- HISTORIQUE TRANSACTIONS --------
        export_webi(
            token,
            WEBI_HISTO_TRX_ID,
            {
                "DATE_DEBUT": date_debut,
                "DATE_FIN": date_fin,
                "CODE_OPERATION": "0500"
            },
            OUTPUT_HISTO / f"Historique_trx_{date_fin}.csv"
        )

        # -------- LANCEMENT SCRIPT MÉTIER --------
        logging.info("Lancement du script point_situation_tpe_auto.py")
        subprocess.run(
            ["python", str(SCRIPT_METIER)],
            check=True
        )

        logging.info("Processus terminé avec succès ✅")

    except Exception as e:
        logging.error(f"ERREUR CRITIQUE : {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
