"""Programme pour régler la température de différentes pièces."""

import pandas as pd
import numpy as np
from meteostat import Hourly, Point
from datetime import datetime, timedelta
from sensors_db_connection import db_connection
import joblib


# Création du jeu pour le modèle IA
def meteo_datas(tableau_pieces, ville, date):
    tableau = tableau_pieces.reset_index(drop= True)
    # Passage de la date en minutes
    # date_m_now = datetime.strptime(date[:-7], "%Y-%m-%d %H:%M")
    date_m_h1 = date - timedelta(hours= 1)

    # Scraping données météo
    m_datas = Hourly(ville, date_m_h1, date)
    m_datas = m_datas.fetch()
    m_datas = m_datas.reset_index(drop= True)

    # Valeurs du jeu
    inlet = tableau.loc[0, "InletTemp-sensor"]
    drive = tableau.loc[0, "Drive-sensor"]
    mode = tableau.loc[0, "Mode-sensor"]
    temp = m_datas.loc[0, "temp"]
    rhum = m_datas.loc[0, "rhum"]
    prcp = m_datas.loc[0, "prcp"]
    wspd = m_datas.loc[0, "wspd"]
    wpgt = m_datas.loc[0, "wpgt"]
    pres = m_datas.loc[0, "pres"]

    return pd.DataFrame([[inlet, temp, rhum, prcp, wspd, wpgt, pres, drive, mode]], 
        columns= ["InletTemp-sensor", "temp", "rhum", "prcp", "wspd", "wpgt", 
        "pres", "Drive-sensor", "Mode-sensor"])


if __name__ == "__main__":
    # Connexion à la base de données et récupération des mesures
    measures_df, sensors_df, engine = db_connection()

    # Création et initialisation du tableau de suivi des données 
    # pour chaque pièces. Ce tableau sera mis à jour à chaque 
    # nouvelle entrée
    tableau_pieces = pd.DataFrame([], columns= 
        ["Drive-sensor", "InletTemp-sensor", "Mode-sensor", "temp", 
        "rhum", "prcp", "wspd", "wpgt", "pres"], index= range(1, 20))

    tableau_pieces["Drive-sensor"].fillna(0.0, inplace= True)
    tableau_pieces["InletTemp-sensor"].fillna(16.0, inplace= True)
    tableau_pieces["Mode-sensor"].fillna(2.0, inplace= True)

    # Création du jeu dans lequel seront intégrés les actions
    actions_df = pd.DataFrame([], columns= 
        ["SensorId", "EquipmentId", "Value", "DateTime"])

    # Chargement du modèle de prédiction et de transformation
    model = joblib.load("settemp_model.sav")
    ssc = joblib.load("settemp_ssc.sav")

    # Ville
    Paris = Point(48.82, 2.34, 75)

    # Parcours des mesures:
    # Si SetTemp-sensor 
    #   => modèle si Drive-sensor == 0
    #   => ajout dans "actions_df" si Drive-sensor == 1
    # Si InletTemp-sensor => MAJ "tableau_pieces"
    # Si Drive-sensor ou Mode-sensor => MAJ "tableau-pieces" 
    # + ajout "actions_df" si modification
    for idx, row in measures_df.iterrows():
        # Capteur
        capteur = (row["SensorId"] -1) % 6
        equipt = ((row["SensorId"] -1) // 6) +1
        # 0 == InletTemp-sensor
        if capteur == 0:
            tableau_pieces.loc[equipt, "InletTemp-sensor"] = row["Value"]
        # 1 == SetTemp-sensor
        elif capteur == 1:
            if tableau_pieces.loc[equipt, "Drive-sensor"] == 0:
                # Création du jeu pour le modèle IA
                model_df = meteo_datas(tableau_pieces, Paris, row["DateTime"])
                # Transformation du jeu
                tmp_cols = [x for x in model_df.columns if x not in ["Drive-sensor", 
                    "Mode-sensor"]]
                df_tmp = pd.DataFrame(ssc.transform(model_df[tmp_cols]), columns= tmp_cols)
                model_df_ssc = pd.concat([df_tmp, model_df[["Drive-sensor", "Mode-sensor"]]], 
                    axis= 1)
                # Prédiction de SetTemp-sensor
                set_pred = np.round(model.predict(model_df_ssc))[0]
            else: set_pred = row["Value"]

            tmp_df = pd.DataFrame([[row["SensorId"], equipt, set_pred, 
                    row["DateTime"]]], columns= ["SensorId", "EquipmentId", 
                    "Value", "DateTime"])
            actions_df = pd.concat([actions_df, tmp_df])

        # 2 == Drive-sensor
        elif capteur == 2:
            if row["Value"] != tableau_pieces.loc[equipt, "Drive-sensor"]:
                tmp_df = pd.DataFrame([[row["SensorId"], equipt, row["Value"], 
                    row["DateTime"]]], columns= ["SensorId", "EquipmentId", 
                    "Value", "DateTime"])
                actions_df = pd.concat([actions_df, tmp_df])

            tableau_pieces.loc[equipt, "Drive-sensor"] = row["Value"]
        # 4 == Mode-sensor
        elif capteur == 4:
            if row["Value"] != tableau_pieces.loc[equipt, "Mode-sensor"]:
                tmp_df = pd.DataFrame([[row["SensorId"], equipt, row["Value"], 
                    row["DateTime"]]], columns= ["SensorId", "EquipmentId", 
                    "Value", "DateTime"])
                actions_df = pd.concat([actions_df, tmp_df])

            tableau_pieces.loc[equipt, "Mode-sensor"] = row["Value"]

        if (idx + 1) % 1000 == 0:
            print(f"{idx +1} / {len(measures_df)} valeurs effectuées")

    actions_df.reset_index(drop= True, inplace= True)
    actions_df.to_sql("test_exo", engine)
