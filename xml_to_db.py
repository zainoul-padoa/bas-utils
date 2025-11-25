import os
import xml.etree.ElementTree as ET
import sqlite3
# === CONFIGURATION ===
XML_DIR = "Archiv"   # répertoire des fichiers XML
DB_FILE = "output.db"           # fichier SQLite (modifiable)
# ======================
# Connexion à la base de données
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()
for filename in os.listdir(XML_DIR):
    if not filename.endswith(".xml"):
        continue
    path = os.path.join(XML_DIR, filename)
    tree = ET.parse(path)
    root = tree.getroot()
    # Nom de la table (à partir du nom du fichier ou de l'attribut XML)
    table_name = root.attrib.get("name") or root.tag or os.path.splitext(filename)[0]
    # Essaye de trouver une ligne (Row ou élément enfant)
    rows = root.findall(".//Row")
    if not rows:
        # Si aucun <Row> trouvé, peut-être que les enfants directs sont les lignes
        children = list(root)
        if children and all(len(list(c)) > 0 for c in children):
            rows = children
    # === CAS XML VIDE ===
    if not rows:
        print(f"[INFO] {filename}: aucun enregistrement trouvé. Création de la table vide '{table_name}'.")
        # On essaie d'inférer les colonnes si possible depuis un exemple vide
        # (Sinon, crée une table vide sans colonnes)
        sample = root.find(".//Row") or (list(root)[0] if len(list(root)) > 0 else None)
        if sample is not None:
            cols_table = [elem.tag for elem in sample]
        else:
            cols_table = []  # aucune colonne détectée
        if cols_table:
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{c} TEXT' for c in cols_table])});"
        else:
            # table sans colonnes explicites
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT);"
        cursor.execute(create_sql)
        conn.commit()
        continue  # passe au fichier suivant
    # === CAS NORMAL (XML NON VIDE) ===
    # Colonnes à partir de la première ligne
    first_row = rows[0]
    cols_table = [elem.tag for elem in first_row]
    # Création de la table si non existante
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{c} TEXT' for c in cols_table])});"
    cursor.execute(create_sql)
    # Insertion de chaque ligne
    for row in rows:
        values = []
        cols_insert = []
        # Traitement de chaque colonne
        for child in row:
            cols_insert.append(child.tag)
            # Rajout d'une nouvelle colonne si besoin
            if child.tag not in cols_table:
                alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {child.tag} TEXT"
                cursor.execute(alter_sql)
                cols_table.append(child.tag)
            values.append(child.text)
        placeholders = ",".join(["?"] * len(cols_insert))
        print(len(values))
        print('and')
        print(len(cols_insert))
        print(table_name)
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols_insert)}) VALUES ({placeholders})"
        cursor.execute(insert_sql, values)
    conn.commit()
    print(f"[OK] Table '{table_name}' : {len(rows)} lignes insérées.")
conn.close()
print("\n:white_check_mark: Base de données recréée avec succès :", DB_FILE)