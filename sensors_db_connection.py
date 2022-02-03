"""Code de connexion à la base de données.
Utilisé dans sensors_proba.py"""

from sqlalchemy  import create_engine
import pandas as pd


def db_connection():
    """Création du moteur de connexion à la base de données"""

    # Dictionnaire des dialects et drivers associées
    dialects_drivers = {"mysql": None, "postgresql": None, "oracle": None, 
        "mssql": "pymssql", "sqlite": None}

    # Boucle jusqu'à obtenir le bon dialect, q pour quitter
    while True:
        while True:
            # Type de base de données et driver associé
            db_type = input(f"Entrer le type de base de données ('q' pour quitter ):"
                f"\nDialect supporté:\n\t"
                f"MySQL => mysql\n\tPostgreSQL => postgresql\n\tOracle => oracle"
                f"\n\tMicrosoft SQL Server => mssql\n\tSQLite => sqlite\n").lower()
            if db_type == "q":
                break
            try:
                driver = dialects_drivers[db_type]
                break
            except Exception as e:
                print(f"Vérifier l'orthographe du type de base de données.\n")
        
        # Si le dialect est sqlite, il faut uniquement le chemin de la base de données
        if db_type == "sqlite":
            path = input(f"Entrer le chemin d'accès à la base de données:\n")
        # Sinon demander le login, mot de passe, ip du serveur, port d'entrée et nom 
        # de la base de données
        else:
            login = input(f"Entrer votre identifiant:\n")
            password = input(f"Entrer votre mot de passe:\n")
            host = input(f"Serveur hébergeant la base de données (taper ENTRÉE pour"
                f"'localhost' par défault):\n")
            if host == "":
                host = "localhost"
            # Imposer un port valide
            while True:
                port = input(f"Port du serveur (taper ENTRÉE pour 3306 par défault):\n")
                if port == "":
                    port = 3306
                    break
                try:
                    port = int(port)
                    if 0 <= port <= 65535:
                        break
                    else:
                        print(f"Le port n'est pas compris entre 0 et 65535 inclus.")
                except:
                    print(f"La valeur entrée n'est pas un nombre.")

            db_name = input(f"Nom de la base de données:\n")

        #Création de la requête de connexion à la base de données
        print(f"Création de la connexion à la base de données...")
        try:
            # Requêtes selon le dialect
            if db_type == "sqlite":
                engine = create_engine(f"{db_type}:///{path}")
            elif db_type == "mssql":
                engine = create_engine(f"{db_type}+{driver}://{login}:{password}"
                    f"@{host}:{port}/{db_name}")
            else:
                engine = create_engine(f"{db_type}://{login}:{password}@{host}:"
                    f"{port}/{db_name}")
        except Exception as e:
            print(f"Vérifier les paramètres de connexion")
            print(f"Log: \n{e}")

        # Entrer la/les requêtes sql pour extraire la/les tables
        # Deux booléens de retour valant True si l'extraction s'est bien déroulée
        # Extraction de la table "DataMeasurements"
        measures_bool = False
        try:
            measures_table = input(f"Entrer le nom de la table contenant les mesures "
                f"(DataMeasurements):\n")
            measures_query = f"SELECT * FROM {measures_table}"
            print(f"Extraction de la table '{measures_table}'...")
            measures_df = pd.read_sql(measures_query, engine)
            measures_bool = True
        except Exception as e:
            measures_bool = False
            print(f"Erreur lors de l'extraction de {measures_table}")
            print(f"Log:\n{e}")
        # Extraction de la table "Sensors"
        sensors_bool = False
        try:
            sensors_table = input(f"Entrer le nom de la table contenant les capteurs "
                f"(Sensors):\n")
            sensors_query = f"SELECT * FROM {sensors_table}"
            print(f"Extraction de la table '{sensors_table}'...")
            sensors_df = pd.read_sql(sensors_query, engine)
            sensors_bool = True
        except Exception as e:
            sensors_bool = False
            print(f"Erreur lors de l'extraction de {sensors_table}")
            print(f"Log:\n{e}")
        
        # Si les booléens précédents valent tous les deux True, le processus s'est entièrement 
        # bien déroulé. Les deux tables au format pandas.DataFrame sont retournées
        if measures_bool and sensors_bool:
            print("Chargment des tables effectué avec succès! ☻")
            return measures_df, sensors_df, engine
        # Sinon demander pour lancer une nouvelle tenantive
        else:
            recommencer = input(f"Tenter une nouvelle connexion? ('q' pour quitter):\n")
            if recommencer.lower() == "q":
                break

