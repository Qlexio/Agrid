"""Code analysant les jeux de données contenant les mesures 
des capteurs et les informations des capteurs. Il effectue 
ensuite les actions nécessaires pour le nettoyer et le formatter."""


import pandas as pd
import numpy as np


def datas_formatting(measures_df, sensors_df):
    """Etraction des données concernant les capteurs.
    Nettoyer et Formatage du jeu de données."""
    
    print(f"\nTraitement des jeux de données...")
    ### Drop des colonnes inutiles de sensors_df
    sensors_df.drop(["Technical_Name", "DataTypeId", "UnitId", "Active", "FatherId", "IdLocal_Hub", 
    "SubType", "TransmissionMode", "DirectUrl", "BoxId", "IdLocal_Setup"], axis= 1, inplace= True)

    ### Merge de measures_df et sensors_df dans mon jeu final
    datas = measures_df.merge(sensors_df[["SensorId", "Name", "EquipmentId"]], on= "SensorId")
    datas.sort_values(by= "DataMeasurementId", inplace= True)
    datas.reset_index(drop= True, inplace= True)

    ### Extraction des valeurs uniques et des intervalles selon le capteur
    # Chaque capteur peut prendre un nombre défini de valeurs:
    # Les capteurs de température sont définis sur un intervalle allant de 
    # 16 à 32 inclus
    # Les autres prennent des valeurs uniques
    print(f"\t1/2 Extration des données relatives aux capteurs...")
    equipt = sensors_df[sensors_df["EquipmentId"] == 1]

    # Création du dictionnaire de récupération des valeurs
    interv_listes = {}
    for capt in equipt["Name"].unique():
        interv_listes[capt] = []
        idx = equipt.loc[equipt["Name"] == capt].index.values[0]
        for col in equipt.columns[-3:]:
            value = equipt.loc[idx, col]
            if type(value) == np.float64:
                if not np.isnan(value):
                    interv_listes[capt].append(value)
            elif value is not None and type(value) == str:
                tmp_vals = value.split(";")
                for tmp_val in tmp_vals:
                    interv_listes[capt].append(tmp_val.split("|")[0])

    ### Extraction des correspondances des valeurs entre les colonnes "Raw" 
    # (faciles à trouver) et "Values" pour les capteurs non thermiques
    str_capteurs = [x for x in datas["Name"].unique() if x not in ["InletTemp-sensor", 
    "SetTemp-sensor"]]
    # Création du dictionnaire récupérant les résultats
    vals_corresp = {}
    for capt, vals in interv_listes.items():
        if capt in str_capteurs:
            vals_corresp[capt] = {}
            for val in vals:
                vals_corresp[capt][val] = datas.loc[(datas["Name"] == capt) & (datas["Raw"] 
                    == val), "Value"].unique().tolist()

    ### Formatage des valeurs
    # Retrait des valeurs nulles
    print(f"\t2/2 Nettoyage et formatage du jeu de données...")
    datas = datas[datas["Value"].notna()]
    # Il faut se baser sur les colonnes "Raw" qui est très explicite. 
    # Pour commencer, il faut formatter les valeurs, c'est-à-dire transformer 
    # les valeurs des capteurs thermiques en nombres et laisser les autres en chaine 
    # de caractères
    # Pour se faire, je crée une liste de récupération des formattages que je 
    # réintègre dans la colonne "Raw"
    truc_list = []
    for idx, row in datas.iterrows():
        # Cas des capteurs thermiques
        if row["Name"] in ["InletTemp-sensor", "SetTemp-sensor"]:
            truc_list.append(np.float64(row["Raw"]))
        # Cas des capteurs non thermiques
        else:
            truc_list.append(row["Raw"])
    datas["Raw"] = truc_list.copy()
    ### Retrait des outliers
    # Il faut se référer au dictionnaire 'interv_listes' contenant les intervalles 
    # et les valeurs uniques pour chaque capteur et retirer les valeurs n'étant 
    # pas définis:
    # dans sur l'intervalle pour les capteurs thermiques
    # par les valeurs uniques pour les autres capteurs
    # Création d'un jeu de données vide dans lequel seront intégrés les résultats
    df = pd.DataFrame([], columns= datas.columns)
    # Boucle pour parcourir chaque individu du dictionnaire
    for key, val in interv_listes.items():
        # Extraction d'un sous-jeu contenant le capteur en cours
        tmp_df = datas[datas["Name"] == key]
        # Cas des capteurs thermiques
        if key in ["InletTemp-sensor", "SetTemp-sensor"]:
            tmp_ddf = tmp_df[(tmp_df["Raw"] >= val[0]) & (tmp_df["Raw"] <= val[1])]
            df = pd.concat([df, tmp_ddf])
        # Cas des capteurs non thermiques
        else:
            tmp_ddf = tmp_df[tmp_df["Raw"].isin(val)]
            df = pd.concat([df, tmp_ddf])

    df = df.sort_values(by= "DateTime").reset_index(drop= True)

    ### Attribution de la différence de temps entre les relevés dans une nouvelle 
    # colonne. Il faut bien penser à effectuer cette étape équipement par équipement
    # pour éviter les biais dans les calculs. Ce qui nous intéresse c'est le temps 
    # entre deux relevés du même équipement.
    # Création d'un jeu de données vide dans lequel seront intrégrés les résultats
    f_datas = pd.DataFrame([], columns= datas.columns)
    # Boucle sur les équipements
    for i in range(1, 20):
        tmp_df = df[df["EquipmentId"] == i].copy()
        # Calcul des écarts de temps
        tmp_df["diff_time"] = pd.to_datetime(tmp_df['DateTime'].astype(str)).diff(1).dt.total_seconds()
        f_datas = pd.concat([f_datas, tmp_df])

    f_datas.reset_index(drop= True, inplace= True) 

    return f_datas, vals_corresp