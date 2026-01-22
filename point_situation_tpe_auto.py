# =============================================================================
# AUTO-GENERATED .py FROM 'POINT DE SITUATION TPE.ipynb'
# =============================================================================
# Notes:
# - This script keeps the notebook cells in the same order (see "# --- Cell X ---").
# - Scheduling: configure Windows Task Scheduler to run this script every Friday at 12:00.
# - The automation hooks below are OPTIONAL and do not delete any notebook logic.
#
# Paths provided by Emmanuel:
DONNEES_TPE_DIR = r"C:\Users\USER\AFRILAND FIRSTBANK\Département Intelligence Artificielle DRI - General\DONNEES TPE"
PBIX_DIR = r"C:\Users\USER\Documents\DASHBOARD TPE"
#
# Optional behavior toggles:
AUTO_DATE_REF = True                 # If True, overrides date_ref to today's date (00:00) after it is defined.
USE_RUN_DATE_FOR_PARC_FILENAME = True # If True, uses execution date (JJMMYYYY) for the "Parc TPE {date} A JOUR.xlsx" filename.
#
# Minimal pre-checks (fail fast if critical folders are missing)
from pathlib import Path
def _precheck_paths():
    missing=[]
    for p in [DONNEES_TPE_DIR, PBIX_DIR]:
        if not Path(p).exists():
            missing.append(p)
    if missing:
        raise FileNotFoundError("Missing required path(s): " + " | ".join(missing))
_precheck_paths()
# =============================================================================


# --- Cell 0 ---
import pandas as pd
import numpy as np
import time
import difflib

# --- Cell 1 ---

# --- Cell 2 ---

# --- Cell 3 ---
import os
import zipfile
import pandas as pd

# 📁 Dossier contenant les fichiers .zip
chemin_dossier = "D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\DATA OP06"  # ← À adapter

# Liste pour stocker les DataFrames extraits
liste_df = []

# Parcours de tous les fichiers dans le dossier
for nom_fichier in os.listdir(chemin_dossier):
    if nom_fichier.endswith(".zip"):
        chemin_zip = os.path.join(chemin_dossier, nom_fichier)
        
        with zipfile.ZipFile(chemin_zip, 'r') as archive:
            for fichier_interne in archive.namelist():
                if fichier_interne.endswith(".csv"):
                    with archive.open(fichier_interne) as f:
                        try:
                            df = pd.read_csv(
                                f,
                                encoding='utf-8',
                                sep=';',            # ← à ajuster si ce n'est pas le bon séparateur
                                skiprows=5,          # ← ignore les 3 premières lignes
                                engine='python'
                            )
                            liste_df.append(df)
                        except Exception as e:
                            print(f"⚠️ Erreur de lecture dans {fichier_interne} (dans {nom_fichier}) : {e}")

# Concaténation
df_concat = pd.concat(liste_df, ignore_index=True)

# Sauvegarde du fichier final
#df_concat.to_csv("csv_combiné_depuis_zip.csv", index=False)

#print("✅ CSV combiné généré avec succès sous 'csv_combiné_depuis_zip.csv'")

# --- Cell 4 ---
len(df_concat)

# --- Cell 5 ---
len(df_concat.drop_duplicates())

# --- Cell 6 ---
df_concat=df_concat.drop_duplicates()

# --- Cell 7 ---
df_concat=df_concat[df_concat['Libellé interface trx src']=="Serveur Pos Iso Aut"]

# --- Cell 8 ---
len(df_concat[df_concat['Libellé interface trx src']=="Serveur Pos Iso Aut"])

# --- Cell 9 ---
#Executer celuic ci
def format_merchant_id(x):
    try:
        # Convertit la valeur en entier puis format sur 9 positions
        return f"{int(float(x)):09d}"
    except (ValueError, TypeError):
        # Si non convertible, laisser NaN
        return np.nan

# Application à la colonne entière
df_concat['Merchant ID formatted'] = df_concat['Merchant ID'].apply(format_merchant_id)

# --- Cell 10 ---
df_concat['Merchant ID formatted']

# --- Cell 11 ---
PARCTPE_ANCIEN=pd.read_excel(r'C:\Users\emmanuel_teinga\AFRILAND FIRSTBANK\Département Intelligence Artificielle DRI - General\DONNEES TPE\Parc TPE 28062025 A JOUR4 2.xlsx')

# --- Cell 12 ---
PARCTPE_ANCIEN=PARCTPE_ANCIEN[['TERMINAL', 'TERMINAL NAME', 'MARCHAND', 'AGENCE', 'COMPTE',
       'CODE CLIENT', 'NOM MARCHAND', 'RESEAU ', 'DATE INSTALLATION',
       'TERMINAL1', 'Sous secteur à Risque', 'Secteur Requalifié',
       'Secteur requalifié1', 'Nb_Opérations', 'POS ID', 'Terminal ID_x',
       'Dernière opération', 'Terminal ID_y', 'Dernière opération >10F',
       'Jours depuis dernière op', 'Jours depuis op >10F', 'SEUIL',
       'GESTIONNAIRE', 'EMAIL GESTIONNAIRE', 'EMAIL ANIMATEUR RESEAU',
       'EMAIL DIRECTEUR RESEAU', 'Matricule client']]

# --- Cell 13 ---
PARCTPE_ANCIEN = PARCTPE_ANCIEN.dropna(subset=['TERMINAL', 'TERMINAL NAME', 'MARCHAND'], how="all")

# --- Cell 14 ---
PARCTPE_ANCIEN.columns

# --- Cell 15 ---
#PARCTPE_ANCIEN=pd.read_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\Parc TPE 28062025 A JOUR4 2.xlsx')

# --- Cell 16 ---

# --- Cell 17 ---
PARCTPE_NOUVEAU=pd.read_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\Parc TPE 21072025.xlsx')

# --- Cell 18 ---
PARCTPE_NOUVEAU['Code marchand1'] = PARCTPE_NOUVEAU['Code marchand'].fillna('').astype(str).str.zfill(9)

# --- Cell 19 ---
#PARCTPE_ANCIEN['TERMINAL1'] = PARCTPE_ANCIEN['TERMINAL'].fillna('').astype(str).str.zfill(8)

# --- Cell 20 ---
PARCTPE_ANCIEN["TERMINAL1"] = (PARCTPE_ANCIEN["TERMINAL1"].astype(float).astype(int).astype(str).str.zfill(8))

# --- Cell 21 ---
PARCTPE_ANCIEN

# --- Cell 22 ---
PARCTPE_NOUVEAU

# --- Cell 23 ---

# --- Cell 24 ---
df_concat2=df_concat[df_concat['Merchant ID formatted'].isin(PARCTPE_NOUVEAU['Code marchand1'])]

# --- Cell 25 ---
df_concat2.head()

# --- Cell 26 ---
#df_concat[df_concat['Merchant ID formatted'].isin(PARCTPE_NOUVEAU['Code marchand1'])].to_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\OPTPE 06072025.xlsx')

# --- Cell 27 ---

# --- Cell 28 ---
df_concat2=df_concat2[df_concat2['Statut trx']=='APPROVAL']

# --- Cell 29 ---
OPTPESEM=df_concat2[df_concat2['Merchant ID formatted'].isin(PARCTPE_NOUVEAU['Code marchand1'])]

# --- Cell 30 ---
OPTPESEM.head()

# --- Cell 31 ---
PARCTPE_NOUVEAU[PARCTPE_NOUVEAU['Code marchand1']=="001110001"]

# --- Cell 32 ---
df_concat2[df_concat2['Merchant ID formatted']=="001110001"]

# --- Cell 33 ---
df_concat2.columns

# --- Cell 34 ---

# --- Cell 35 ---
#len(df_concat[df_concat['Merchant ID formatted'].isin(PARCTPE_NOUVEAU['Code marchand1'])])

# --- Cell 36 ---

# --- Cell 37 ---
df_concat2['Mnt trx1']=df_concat2['Mnt trx'].str.replace('\xa0', '').str.replace(',', '.').astype(float)

# --- Cell 38 ---
df_concat2['Date trx carte'] = pd.to_datetime(df_concat2['Date trx carte'], errors='coerce', dayfirst=True)
df_concat2['Semaine'] = df_concat2['Date trx carte'].dt.isocalendar().week
df_concat2['Semaine1'] = (df_concat2['Date trx carte'] - pd.Timedelta(days=4)).dt.isocalendar().week

# --- Cell 39 ---

# --- Cell 40 ---
#df_concat2.to_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\OPTPE 08072025.xlsx')

# --- Cell 41 ---

# --- Cell 42 ---
OPSEM = (
    df_concat2
    .groupby(['Semaine1'])
    .agg(Nb_Opérations=('Mnt trx1', 'count'),
         Volume_Opérations=('Mnt trx1', 'sum'))
    .reset_index()
)

# --- Cell 43 ---
OPSEM

# --- Cell 44 ---
PARCTPE_NOUVEAU

# --- Cell 45 ---
PARCTPE_NOUVEAU.columns

# --- Cell 46 ---
PARCTPE_NOUVEAU['Terminal ID'] = PARCTPE_NOUVEAU['POS ID'].fillna('').astype(str).str.zfill(8)
PARCTPE_NOUVEAU['POS ID'] = PARCTPE_NOUVEAU['POS ID'].fillna('').astype(str).str.zfill(8)

# --- Cell 47 ---
OPTPESEM_NOUVEAUX = OPTPESEM[~OPTPESEM['Terminal ID'].isin(PARCTPE_ANCIEN['TERMINAL1'])]

# --- Cell 48 ---
OPTPESEM_NOUVEAUX

# --- Cell 49 ---
OPTPESEM_NOUVEAUX.columns

# --- Cell 50 ---
OPTPESEM_NOUVEAUX['Date trx carte'] = pd.to_datetime(OPTPESEM_NOUVEAUX['Date trx carte'], errors='coerce', dayfirst=True)

# --- Cell 51 ---
NEWTPE = (
    OPTPESEM_NOUVEAUX
    .groupby(['Terminal ID','Merchant ID'])
    .agg(Nb_Opérations=('Terminal ID', 'count'),
         Date_déploiement=('Date trx carte', 'min'))
    .reset_index()
)

# --- Cell 52 ---
NEWTPE=pd.merge(NEWTPE, PARCTPE_NOUVEAU[['POS ID','Terminal ID', 'POS name','Nom marchand']], on='Terminal ID')

# --- Cell 53 ---
# Supprimer les lignes dont le Merchant ID a exactement 8 caractères
# Supprime les lignes où 'Merchant ID' contient un point
NEWTPE = NEWTPE[~NEWTPE['Merchant ID'].astype(str).str.contains('\.', regex=True)]

# --- Cell 54 ---
NEWTPE_renamed = NEWTPE.rename(columns={
    'Terminal ID': 'TERMINAL',
    'Merchant ID': 'MARCHAND',
    'POS name': 'TERMINAL NAME',
    'Nom marchand': 'NOM MARCHAND',
    'Date_déploiement': 'DATE INSTALLATION'
})

# --- Cell 55 ---
df_empile = pd.concat([PARCTPE_ANCIEN, NEWTPE_renamed], join="outer", ignore_index=True)

# --- Cell 56 ---
df_empile['POS ID'] = df_empile['TERMINAL'].fillna('').astype(str).str.zfill(8)

# --- Cell 57 ---
df_empile

# --- Cell 58 ---
df_empile.columns

# --- Cell 59 ---

# --- Cell 60 ---

# --- Cell 61 ---
TPEPARC

# --- Cell 62 ---
TPEPARC=df_empile

# --- Cell 63 ---
TPEOP=df_concat2

# --- Cell 64 ---
TPEPARC.columns

# --- Cell 65 ---
#Date de dernière opération
# Date de référence (modifiable)
date_ref = pd.to_datetime("2026-01-08")

# Conversion des formats
#TPEOP['Date trx carte'] = pd.to_datetime(TPEOP['Date trx carte'], dayfirst=True, errors='coerce')
#TPEOP['Mnt trx'] = pd.to_numeric(TPEOP['Mnt trx'], errors='coerce')

# Assurez-vous que les Terminal ID sont comparables (texte, 8 chiffres)
#TPEPARC['TERMINAL1'] = TPEPARC['TERMINAL'].astype(str).str.zfill(8)
TPEPARC["TERMINAL1"] = (TPEPARC["TERMINAL"].astype(float).astype(int).astype(str).str.zfill(8))
TPEOP['Terminal ID'] = TPEOP['Terminal ID'].astype(str).str.zfill(8)

TPEPARC=TPEPARC[['TERMINAL', 'TERMINAL NAME', 'MARCHAND', 'AGENCE', 'COMPTE',
       'CODE CLIENT', 'NOM MARCHAND', 'RESEAU ', 'DATE INSTALLATION',
       'TERMINAL1','Sous secteur à Risque', 'Secteur Requalifié','Secteur requalifié1', 'Nb_Opérations','POS ID', 'GESTIONNAIRE', 'EMAIL GESTIONNAIRE', 'EMAIL ANIMATEUR RESEAU',
       'EMAIL DIRECTEUR RESEAU']]
# Filtrer les opérations pour ne garder que celles des TPE du parc
#ops_parc = TPEOP[TPEOP['Terminal ID'].isin(TPEPARC['TERMINAL'])]
ops_parc=TPEOP

# Dernière opération TOUT MONTANT
dernieres_ops = ops_parc.groupby('Terminal ID')['Date trx carte'].max().reset_index()
dernieres_ops = dernieres_ops.rename(columns={'Date trx carte': 'Dernière opération'})

# Dernière opération > 10F
ops_sup_10 = ops_parc[ops_parc['Mnt trx1'] > 10]
dernieres_sup10 = ops_sup_10.groupby('Terminal ID')['Date trx carte'].max().reset_index()
dernieres_sup10 = dernieres_sup10.rename(columns={'Date trx carte': 'Dernière opération >10F'})

# Fusion avec le parc
parc_complet = TPEPARC.merge(dernieres_ops,left_on="TERMINAL1", right_on="Terminal ID", how='left')
parc_complet = parc_complet.merge(dernieres_sup10, left_on="TERMINAL1", right_on="Terminal ID", how='left')

parc_complet['Jours depuis dernière op'] = (date_ref - pd.to_datetime(parc_complet['Dernière opération'])).dt.days
parc_complet['Jours depuis op >10F'] = (date_ref - pd.to_datetime(parc_complet['Dernière opération >10F'])).dt.days


parc_complet['TERMINAL'] = parc_complet['TERMINAL'].astype(str).str.zfill(8)
parc_complet['MARCHAND'] = parc_complet['MARCHAND'].astype(str).str.zfill(9)

# Résultat final
print(parc_complet.head())

# (optionnel) Enregistrement dans un fichier
# parc_complet.to_csv("parc_avec_dates_ops.csv", index=False)

# [AUTO HOOK] Optionally override date_ref with today's date (Windows clock).
# Keeps the original line above intact.
if 'AUTO_DATE_REF' in globals() and AUTO_DATE_REF:
    import pandas as pd
    date_ref = pd.Timestamp.today().normalize()

# --- Cell 66 ---
parc_complet.columns

# --- Cell 67 ---
parc_complet['POS ID'] = TPEPARC['POS ID'].fillna('').astype(str).str.zfill(8)

# --- Cell 68 ---
import pandas as pd

# On récupère la dernière date des transactions
last_date = TPEOP['Date trx carte'].max()

# On la formate en JJMMYYYY (exemple : 31082025)
last_date_str = last_date.strftime("%d%m%Y")

# [AUTO HOOK] Optionally use the execution date (JJMMYYYY) for the Parc filename.
if 'USE_RUN_DATE_FOR_PARC_FILENAME' in globals() and USE_RUN_DATE_FOR_PARC_FILENAME:
    from datetime import date as _date
    last_date_str = _date.today().strftime("%d%m%Y")

# Construire le chemin de sortie dynamiquement
output_path = fr"D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\Parc TPE {last_date_str} A JOUR.xlsx"

# Exporter le fichier Excel
parc_complet.to_excel(output_path, index=False)

# --- Cell 69 ---
#parc_complet.to_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\Parc TPE 31082025 A JOUR.xlsx')

# --- Cell 70 ---
TPEOP.columns

# --- Cell 71 ---

# --- Cell 72 ---
#RETROCESSION DES FONDS Clients 1307

# --- Cell 73 ---
import pandas as pd
from pathlib import Path

# Dossier source
FOLDER = Path(
    r"D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\DATA OP06\OPERATIONS\OP RETRO"
)

# Encodages à tester
encs = ["utf-8", "utf-8-sig", "cp1252", "latin-1", "iso-8859-1"]

dfs = []
files_read = []      # fichiers lus avec succès
files_failed = {}    # fichiers échoués + raison

csv_files = sorted(FOLDER.glob("*.csv"))
print(f"📂 Fichiers CSV trouvés : {len(csv_files)}\n")

for p in csv_files:
    read_ok = False
    last_error = None

    for enc in encs:
        try:
            df = pd.read_csv(
                p,
                engine="python",
                encoding=enc,
                sep=";",
                header=0
            )

            # Ajout traçabilité
            df["__source_file"] = p.name

            dfs.append(df)
            files_read.append(p.name)
            read_ok = True
            break

        except Exception as e:
            last_error = str(e)

    if not read_ok:
        files_failed[p.name] = last_error

# Fusion finale
if dfs:
    OPRETRO = pd.concat(dfs, ignore_index=True, sort=True)
else:
    OPRETRO = pd.DataFrame()

# ----------------------
# 📊 Résumé d'exécution
# ----------------------
print("✅ Fichiers lus et fusionnés :", len(files_read))
print("❌ Fichiers ignorés         :", len(files_failed))
print("📐 OPRETRO shape            :", OPRETRO.shape)

# Détail des fichiers ignorés
if files_failed:
    print("\n🚨 Détails des fichiers non pris en compte :")
    for f, err in files_failed.items():
        print(f" - {f} → {err[:200]}")

# --- Cell 74 ---
import pandas as pd
from pathlib import Path

FOLDER = Path(r"D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\DATA OP06\OPERATIONS\OP RETRO")
encs = ["utf-8","utf-8-sig","cp1252","latin-1","iso-8859-1"]

dfs = []
for p in sorted(FOLDER.glob("*.csv")):
    for enc in encs:
        try:
            df = pd.read_csv(p, engine="python", encoding=enc,  sep=";", header=0)
            df["__source_file"] = p.name
            dfs.append(df)
            break
        except Exception:
            continue

OPRETRO = pd.concat(dfs, ignore_index=True, sort=True) if dfs else pd.DataFrame()
print("OPRETRO shape:", OPRETRO.shape)

# --- Cell 75 ---
OPRETRO.head()

# --- Cell 76 ---
OPRETRO[OPRETRO['Code client']==257449]

# --- Cell 77 ---
#Opération Retrocession
#OPRETRO = pd.read_csv(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\DATA OP06\OPERATIONS\Clients.csv', sep=";", header=0)

# --- Cell 78 ---
#OPRETROx = pd.read_csv(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\DATA OP06\OPERATIONS\Clients 0210.csv', sep=";", header=0)

# --- Cell 79 ---
OPRETRO=OPRETRO.drop_duplicates()

# --- Cell 80 ---
OPRETRO.groupby(['Code operation']).count()

# --- Cell 81 ---
OPRETRO.columns

# --- Cell 82 ---
OPTPE=df_concat2

# --- Cell 83 ---
#OPTPE['Date trx carte'] = pd.to_datetime(OPTPE['Date trx carte'], dayfirst=True, errors='coerce')
#OPTPE['Date_code'] = OPTPE['Date trx carte'].dt.strftime('%y%m%d')

# --- Cell 84 ---
# Création de la nouvelle variable par concaténation
OPTPE['Concat_Num'] = OPTPE['Numéro masqué carte'].astype(str).str[-4:]  + OPTPE['Num author trx'].astype(str)

# Affichage des premières lignes pour vérifier
print(OPTPE[['Num author trx', 'Numéro masqué carte', 'Concat_Num']].head())

# --- Cell 85 ---
OPTPE[OPTPE["Semaine"]==1]

# --- Cell 86 ---
import pandas as pd
import numpy as np

def extraire_variable(texte):
    # NaN / None -> NaN en sortie
    if pd.isna(texte):
        return np.nan
    
    s = str(texte)
    suffixe = s[-6:]           # même logique que ton code d'origine
    parties = s.strip().split()  # gère espaces multiples / tabulations

    # Besoin d'au moins 2 tokens pour parties[1]
    if len(parties) < 2:
        return np.nan

    # Si après le premier espace on a "MO", on prend les 4 caractères après le 2e espace
    if parties[1] == "MO":
        if len(parties) > 2:
            prefixe = parties[2][:4]
        else:
            return np.nan  # pas de 3e token -> motif incomplet
    else:
        prefixe = parties[1][:4]

    return prefixe + suffixe

# application
OPRETRO["Var_concat"] = OPRETRO["Libelle Mvt Hist"].apply(extraire_variable)

col = "Libelle Mvt Hist"
# 1) Ne garder que les lignes dont le libellé commence par un des préfixes
prefixes = ("GIMA", "VISA", "PAIEMENT", "MAST")
mask_start = OPRETRO[col].astype(str).str.startswith(prefixes)
OPRETRO = OPRETRO[mask_start].copy()
OPRETRO["Var_concat"] = OPRETRO["Libelle Mvt Hist"].apply(extraire_variable)

print(OPRETRO[['Libelle Mvt Hist', 'Var_concat']])

# --- Cell 87 ---
def extraire_variable(texte):
    parties = str(texte).strip().split()  #texte.split(' ') 
    
        # Si après le premier espace on a "MO", on prend les 4 caractères après le 2e espace
    if parties[1]=="MO":
        prefixe = parties[2][:4]
        
    else:
            # Sinon on prend les 4 caractères après le 1er espace
        prefixe = parties[1][:4]
       
        
    suffixe = texte[-6:]
    
    return prefixe + suffixe

# Appliquer la fonction au DataFrame
#OPRETRO=OPRETRO[OPRETRO['Code operation']!=68]
col = "Libelle Mvt Hist"

# 1) Ne garder que les lignes dont le libellé commence par un des préfixes
prefixes = ("GIMA", "VISA", "PAIEMENT", "MAST")
mask_start = OPRETRO[col].astype(str).str.startswith(prefixes)
OPRETRO = OPRETRO[mask_start].copy()
OPRETRO["Var_concat"] = OPRETRO["Libelle Mvt Hist"].apply(extraire_variable)

print(OPRETRO[['Libelle Mvt Hist', 'Var_concat']])

# --- Cell 88 ---
import pandas as pd

def extraire_variable(texte):
    # Sécuriser NaN / None et normaliser en str
    if pd.isna(texte):
        return None
    s = str(texte).strip()
    if not s:
        return None

    parts = s.split()

    # Déterminer le préfixe selon la règle:
    # - si le 2e token == "MO" et on a au moins 3 tokens -> 4 premiers chars du 3e token
    # - sinon, si on a au moins 2 tokens -> 4 premiers chars du 2e token
    # - sinon -> None
    prefixe = None
    if len(parts) >= 3 and parts[1] == "MO":
        prefixe = parts[2][:4]
    elif len(parts) >= 2:
        prefixe = parts[1][:4]
    else:
        prefixe = None

    # Suffixe = 6 derniers caractères de la chaîne originale
    suffixe = s[-6:] if len(s) >= 1 else ""

    # Si l’un des deux manque, retourne None (ou adapte selon ton besoin)
    if not prefixe:
        return None

    return f"{prefixe}{suffixe}"

# -------- Application au DataFrame --------
col = "Libelle Mvt Hist"

# 1) Ne garder que les lignes dont le libellé commence par un des préfixes
prefixes = ("GIMA", "VISA", "PAIEMENT", "MAST")
mask_start = OPRETRO[col].astype(str).str.startswith(prefixes)
OPRETRO = OPRETRO[mask_start].copy()

# 2) Calcul de Var_concat sans lever d'IndexError
OPRETRO["Var_concat"] = OPRETRO[col].apply(extraire_variable)

print(OPRETRO[[col, "Var_concat"]])

# --- Cell 89 ---

# --- Cell 90 ---

# --- Cell 91 ---
# Extraction et concaténation
#OPRETRO['Var_concat'] = (
 #   OPRETRO['Libelle Mvt Hist']
  #  .str.extract(r'MO (\d{4})')[0] 
   # + OPRETRO['Libelle Mvt Hist'].str[-6:]


# Affichage du résultat
OPRETRO[['Libelle Mvt Hist', 'Var_concat']].head()

# --- Cell 92 ---
OPRETRO1=OPRETRO[OPRETRO["Code client"]<2000000]

# --- Cell 93 ---
OPRETRO1=OPRETRO1[OPRETRO1["Sens Mvt Hist"]=='C']

# --- Cell 94 ---
OPRETRO1['Source'] = OPRETRO1['Libelle Mvt Hist'].str.split(' ').str[0]

# --- Cell 95 ---
OPRETRO1.columns

# --- Cell 96 ---
OPRETRO1[OPRETRO1['Var_concat']=="5155884394"]

# --- Cell 97 ---
OPRETRO1.drop_duplicates()

# --- Cell 98 ---
OPTPE2=OPTPE

# --- Cell 99 ---
#OPTPE2['Mnt trx1'] = OPTPE2['Mnt trx1'].astype(float)
#OPRETRO1['Montant Mvt Hist'] = OPRETRO1['Montant Mvt Hist'].astype(float)

# --- Cell 100 ---

# --- Cell 101 ---
OPTPE2

# --- Cell 102 ---
#matching sans le montant de l'opération
#OPTPE2=pd.merge(OPTPE2, OPRETRO1[["Var_concat","Date comptable","Source"]], left_on="Concat_Num", right_on="Var_concat", how='left').drop_duplicates()
OPTPE2['Mnt trx1'] = OPTPE2['Mnt trx'].str.replace('\xa0', '').str.replace(',', '.').astype(float)
#OPRETRO1['Montant Mvt Hist'] = OPRETRO1['Montant Mvt Hist'].str.replace('\xa0', '').str.replace(',', '.').astype(float)


# Étape 1 : merge standard sur la clé
OPTPE2 = pd.merge(
    OPTPE2,
    OPRETRO1[['Var_concat', 'Date comptable', 'Source','Montant Mvt Hist']],
    left_on=['Concat_Num'],
    right_on=['Var_concat'],
    how='left'
)

# Étape 2 : conversions si nécessaire
#OPTPE2['Date trx carte'] = pd.to_datetime(OPTPE2['Date trx carte'], errors='coerce')
OPTPE2['Date comptable'] = pd.to_datetime(OPTPE2['Date comptable'], errors='coerce', dayfirst=True)

# Étape 3 : appliquer la condition ligne à ligne et remplacer les valeurs par NaN si la condition échoue
condition_valide = OPTPE2['Date comptable'] >= OPTPE2['Date trx carte']
OPTPE2.loc[~condition_valide, ['Var_concat','Date comptable', 'Source','Montant Mvt Hist']] = pd.NA


OPTPE2['Duree'] = np.nan

# Masque pour sélectionner les lignes valides
masque_valide = OPTPE2['Date trx carte'].notna() & OPTPE2['Date comptable'].notna()

# Calculer le nombre de jours ouvrés seulement pour les lignes valides
OPTPE2.loc[masque_valide, 'Duree'] = np.busday_count(
    OPTPE2.loc[masque_valide, 'Date trx carte'].values.astype('datetime64[D]'),
    OPTPE2.loc[masque_valide, 'Date comptable'].values.astype('datetime64[D]')
)

# --- Cell 103 ---
OPTPE2['Duree'] = np.nan

# Masque pour sélectionner les lignes valides
masque_valide = OPTPE2['Date trx carte'].notna() & OPTPE2['Date comptable'].notna()

# Calculer le nombre de jours ouvrés seulement pour les lignes valides
OPTPE2.loc[masque_valide, 'Duree'] = np.busday_count(
    OPTPE2.loc[masque_valide, 'Date trx carte'].values.astype('datetime64[D]'),
    OPTPE2.loc[masque_valide, 'Date comptable'].values.astype('datetime64[D]')
)

# --- Cell 104 ---

OPTPE2.drop_duplicates(["Concat_Num"]).to_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\DATA OP06\OPERATIONS\OPTPE PAIEM RETROCEDES TEST 10012026.xlsx')

# --- Cell 105 ---
OPTPE2.drop_duplicates(["Concat_Num"]).head(5)

# --- Cell 106 ---

# --- Cell 107 ---

# --- Cell 108 ---
OPTPE3=OPTPE

# --- Cell 109 ---
# Étape 1 : merge uniquement sur la clé concaténée
df_compare = pd.merge(
    OPTPE3,
    OPRETRO1[['Var_concat', 'Montant Mvt Hist']],
    left_on='Concat_Num',
    right_on='Var_concat',
    how='left',  # inner pour garder uniquement les correspondances
    suffixes=('', '_retro')
)

# Étape 2 : comparer les montants (≠)
# Tu peux convertir les deux colonnes si besoin pour éviter les erreurs
#df_compare['Mnt trx1'] = df_compare['Mnt trx'].str.replace('\xa0', '').str.replace(',', '.').astype(float)
#df_compare['Montant Mvt Hist'] = df_compare['Montant Mvt Hist'].str.replace('\xa0', '').str.replace(',', '.').astype(float)

# --- Cell 110 ---
df_compare['Montant Mvt Hist'] = (
    df_compare['Montant Mvt Hist']
    .astype(str)
    .str.replace('\xa0', '', regex=False)
    .str.replace(',', '.', regex=False)
    .astype(float)
)

# --- Cell 111 ---
df_compare.drop_duplicates(["Concat_Num"]).to_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\DATA OP06\OPERATIONS\OPTPE PAIEM RETROCEDES 27072025.xlsx')

# --- Cell 112 ---
# Étape 3 : garder uniquement les lignes avec montants différents
transactions_montants_differents = df_compare[
    df_compare['Mnt trx1'] != df_compare['Montant Mvt Hist']
]

# --- Cell 113 ---
transactions_montants_differents.drop_duplicates().to_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\DATA OP06\OPERATIONS\OPTPE PAIEM RETROCEDES MONTANTS DIFFERENTS 27072025.xlsx')

# --- Cell 114 ---

# --- Cell 115 ---
df_compare

# --- Cell 116 ---
OPTPE3=OPTPE2.drop_duplicates(["Var_concat","Concat_Num"])

# --- Cell 117 ---
OPTPE3=OPTPE2

# --- Cell 118 ---
len(OPTPE2)

# --- Cell 119 ---
len(OPTPE3)

# --- Cell 120 ---
# Conversion de la date de fin
#OPTPE3['Date comptable1'] = pd.to_datetime(OPTPE3['Date comptable'], errors='coerce', dayfirst=True)


# 🛠️ S'assurer que les dates sont bien interprétées avec le jour en premier
#OPTPE3['Date trx carte'] = pd.to_datetime(OPTPE3['Date trx carte'], errors='coerce')
#OPTPE3['Date comptable'] = pd.to_datetime(OPTPE3['Date comptable'], errors='coerce')

# Créer une colonne vide pour les résultats
OPTPE3['Duree'] = np.nan

# Masque pour sélectionner les lignes valides
masque_valide = OPTPE3['Date trx carte'].notna() & OPTPE3['Date comptable'].notna()

# Calculer le nombre de jours ouvrés seulement pour les lignes valides
OPTPE3.loc[masque_valide, 'Duree'] = np.busday_count(
    OPTPE3.loc[masque_valide, 'Date trx carte'].values.astype('datetime64[D]'),
    OPTPE3.loc[masque_valide, 'Date comptable'].values.astype('datetime64[D]')
)

# --- Cell 121 ---

# --- Cell 122 ---

# --- Cell 123 ---
OPTPE3[['Nom porteur carte','Numéro masqué carte','Num author trx','Libelle type trx','Statut trx','Date trx carte','Terminal ID', 'Nom terminal', 'Merchant ID', 'Mnt trx1', 'Semaine', 'Concat_Num',
       'Var_concat', 'Date comptable','Duree', 'Source']].drop_duplicates().to_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\DATA OP06\OPERATIONS\OPTPE DATERETRO3ALL1 13072025.xlsx')

# --- Cell 124 ---

# --- Cell 125 ---
OPTPE4=OPTPE3

# --- Cell 126 ---

# --- Cell 127 ---
# Exemple avec une colonne nommée 'MaColonne'
nb_nulls = OPTPE3.drop_duplicates(["Var_concat","Concat_Num"])['Duree'].isnull().sum()
print(f"Nombre de lignes avec valeur nulle dans 'MaColonne' : {nb_nulls}")

# --- Cell 128 ---

# --- Cell 129 ---

# --- Cell 130 ---

# --- Cell 131 ---

# --- Cell 132 ---

# --- Cell 133 ---
len(TPEOP0106)

# --- Cell 134 ---
#Les opérations sur les TPE du parc
TPEOP0106A=TPEOP0106[TPEOP0106['Terminal ID'].isin(parc_complet1["POS ID"])]

# --- Cell 135 ---
TPEOP0106A=TPEOP

# --- Cell 136 ---
len(TPEOP0106PARC)

# --- Cell 137 ---
TPEOP0106PARC1=TPEOP0106PARC[TPEOP0106PARC['Statut trx']=='APPROVAL'].drop_duplicates()

# --- Cell 138 ---
TPEOP0106PARC1['Mnt trx1'].sum()

# --- Cell 139 ---

# --- Cell 140 ---
TPEOP0106PARC[TPEOP0106PARC['Statut trx']=='APPROVAL'].drop_duplicates()['Mnt trx1'].sum()

# --- Cell 141 ---
TPEOP0106A['Mnt trx1'] = pd.to_numeric(TPEOP0106A['Mnt trx'], errors='coerce')

# --- Cell 142 ---
TPEOP0106PARC['Mnt trx1']=TPEOP0106PARC['Mnt trx'].str.replace('\xa0', '').str.replace(',', '.').astype(float)

# --- Cell 143 ---
TPEOP0106AR=TPEOP0106A[TPEOP0106A['Statut trx']=='APPROVAL']

# --- Cell 144 ---
TPEOP0106AR['Date trx carte'] = pd.to_datetime(TPEOP0106AR['Date trx carte'], errors='coerce', dayfirst=True)
TPEOP0106AR['Semaine'] = TPEOP0106AR['Date trx carte'].dt.isocalendar().week

# --- Cell 145 ---
OPTPESEM = (
    TPEOP0106AR
    .groupby(['Terminal ID', 'Semaine'])
    .agg(Nb_Opérations=('Mnt trx1', 'count'),
         Volume_Opérations=('Mnt trx1', 'sum'))
    .reset_index()
)

# --- Cell 146 ---
OPSEM = (
    TPEOP0106AR
    .groupby([ 'Semaine'])
    .agg(Nb_Opérations=('Mnt trx1', 'count'),
         Volume_Opérations=('Mnt trx1', 'sum'))
    .reset_index()
)

# --- Cell 147 ---
OPSEM['Nb_Opérations'].sum()

# --- Cell 148 ---
OPSEM['Volume_Opérations'].sum()

# --- Cell 149 ---

# --- Cell 150 ---

# --- Cell 151 ---
len(TPEOP0106A)

# --- Cell 152 ---
len(TPEOP0106AR)

# --- Cell 153 ---
TPEOP0106A['Statut trx']

# --- Cell 154 ---
TPEOP0106AR['Mnt trx1'].sum()

# --- Cell 155 ---
parc_complet2=parc_complet1[parc_complet1["POS ID"].isin(TPEDEP['ID_CLIENT'])

# --- Cell 156 ---

# --- Cell 157 ---
parc_complet.to_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\Parc TPE 01072025 A JOUR5.xlsx')

# --- Cell 158 ---
TPEOP0106A.to_excel(r'D:\Documents EMMANUEL\Documents EMMANUEL\IA\DEPLOIEMENT DE 500 TPE\CAMPAGNE DE DEPLOIEMENT\POINT DE SITUATION\OPTPE 30062025.xlsx')

# --- Cell 159 ---
TPEPARC.columns

# --- Cell 160 ---

# --- Cell 161 ---

# --- Cell 162 ---
Prod=pd.read_csv(r'C:\Users\emmanuel_teinga\Downloads\Productivité certifié\Comptes.csv', sep=";")

# --- Cell 163 ---
Prod['Mnt trx1']=Prod['Productivité hors leasing'].str.replace('\xa0', '').str.replace(',', '.').astype(float)

# --- Cell 164 ---
import pandas as pd

df=Prod
# df doit contenir au minimum: ['Code client', 'Mnt trx1']
# Exemple: df = pd.read_csv('transactions.csv')
df['Mnt trx1'] = pd.to_numeric(df['Mnt trx1'], errors='coerce').fillna(0)

# 1) Somme par client et tri décroissant
client_sum = (df.groupby('Coed client', as_index=False)['Mnt trx1']
                .sum().rename(columns={'Mnt trx1':'client_total'})
                .sort_values('client_total', ascending=False))

# 2) Part cumulée et seuil 80 %
total = client_sum['client_total'].sum()
client_sum['cum_share'] = client_sum['client_total'].cumsum() / total
client_sum['top80_flag'] = (client_sum['cum_share'] <= 0.80).astype(int)

# 3) Ramener le flag sur chaque transaction
df = df.merge(client_sum[['Coed client','top80_flag']], on='Coed client', how='left').fillna({'top80_flag':0})

# 4) Moyennes au niveau transaction
avg_tx_top80   = df.loc[df['top80_flag']==1, 'Mnt trx1'].mean()
avg_tx_others  = df.loc[df['top80_flag']==0, 'Mnt trx1'].mean()

# 5) Moyennes au niveau client (moyenne des totaux par client)
avg_client_top80  = client_sum.loc[client_sum['top80_flag']==1, 'client_total'].mean()
avg_client_others = client_sum.loc[client_sum['top80_flag']==0, 'client_total'].mean()

print({
  'AVG Mnt trx1 (transactions) - Top80': avg_tx_top80,
  'AVG Mnt trx1 (transactions) - Autres': avg_tx_others,
  'AVG Mnt trx1 (par client) - Top80': avg_client_top80,
  'AVG Mnt trx1 (par client) - Autres': avg_client_others
})

# --- Cell 165 ---
import pandas as pd

# df contient au moins: ['Code client','Mnt trx1']
df['Mnt trx1'] = pd.to_numeric(df['Mnt trx1'], errors='coerce').fillna(0)

client = (df.groupby('Coed client', as_index=False)['Mnt trx1']
            .sum().rename(columns={'Mnt trx1':'client_total'})
            .sort_values('client_total', ascending=False)
            .reset_index(drop=True))

total = client['client_total'].sum()
client['cum_share'] = client['client_total'].cumsum() / total

cutoff_idx = client['cum_share'].ge(0.80).idxmax()  # premier client qui fait atteindre ≥80%
client['top80'] = (client.index <= cutoff_idx).astype(int)

proportion_top80 = client['top80'].mean()  # entre 0 et 1
print("Proportion clients 20/80 =", proportion_top80)         # ex: 0.23
print("Proportion en % =", proportion_top80 * 100)            # ex: 23.0

# --- Cell 166 ---
Prod.columns

# --- Cell 167 ---

# --- Cell 168 ---