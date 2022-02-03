"""Code analysant le jeu de données et calculant les probabilités 
d'apparitions des valeurs"""


import pandas as pd
import numpy as np


def proba_analysis(f_datas, vals_corresp):
    """Analyse les fréquences des valeurs des différents capteurs
    pour chaque équipement.
    Renvoie la probabilité d'apparition du capteur en fonction du 
    capteur de précédent relevé et de la valeur de l'entrée actuelle."""

    # Création du dictionnaire dans lequel seront intégrées les probabilités
    results_dict = {}
    for capt in f_datas["Name"].unique():
        results_dict[capt] = {}

    # Par souci de cohérence des données, parcours des équipements un par un
    for i in range(1, 20):
        tmp_df = f_datas[f_datas["EquipmentId"] == i].reset_index(drop= True)

        # Extraction de la première value de "SetTemp-sensor" dans le sous-jeu
        start = tmp_df[tmp_df["Name"] == "SetTemp-sensor"].iloc[0].name
        set_val = tmp_df.loc[start, "Value"]
        # Parcours des individus
        for j in range(start, len(tmp_df)):
            # Récupération du capteur actuel et du précédent
            actual_capt = tmp_df.loc[j, "Name"]
            prev_capt = tmp_df.loc[j -1, "Name"]

            # Si la clé/valeur n'existe pas dans le dictionnaire, création de 
            # cette entrée
            if results_dict[prev_capt].get(actual_capt) is None \
                and actual_capt in ["InletTemp-sensor", "SetTemp-sensor"]:
                results_dict[prev_capt][actual_capt] = 0
            if results_dict[prev_capt].get(actual_capt) is None \
                and actual_capt in ["Drive-sensor", "FanSpeed-sensor", "Mode-sensor"]:
                results_dict[prev_capt][actual_capt] = {}
                for v in vals_corresp[actual_capt].values():
                    if len(v) > 0:
                        results_dict[prev_capt][actual_capt][v[0]] = 0

            # Récupération de la valeur de "Value"
            value = tmp_df.loc[j, "Value"]
            
            # Si "Value" est compris entre 16 et 32 inclus, il s'agit d'une entrée 
            # d'un capteur thermique
            if 16 <= value <= 32:
                if value == int(value):
                    # Si "Value" est une valeur entière, je regarde la différence 
                    # de temps avec le relevé précédent
                    # Si le temps est inférieur à 2s, il s'agit de "SetTemp-sensor"
                    if tmp_df.loc[j, "diff_time"] <= 2:
                        results_dict[prev_capt]["SetTemp-sensor"] += 1
                    else:
                        # Sinon je fais la différence entre la valeur précédente de 
                        # "SetTemp_sensor" et "Value"
                        difference = set_val - value
                        # Si la différence est un nombre décimal, il s'agit de "InletTemp-sensor"
                        if difference == int(difference):
                            results_dict[prev_capt][actual_capt] += 1
                        else:
                            results_dict[prev_capt]["InletTemp-sensor"] += 1
                else:
                    # Si "Value" est une valeur décimale, il s'agit de "InletTemp-sensor"
                    results_dict[prev_capt]["InletTemp-sensor"] += 1
            # Les autres capteurs ont des valeurs dans l'ensemble {0, 1, 2, 4}
            elif value == 0:
                results_dict[prev_capt]["Drive-sensor"][0] += 1
            elif value == 1:
                results_dict[prev_capt][actual_capt][1] += 1
            elif value == 2:
                results_dict[prev_capt]["Mode-sensor"][2] += 1
            elif value == 4:
                results_dict[prev_capt][actual_capt][4] += 1

            # Mise à jour de la valeur actuelle de "SetTemp-sensor"
            if actual_capt == "SetTemp-sensor":
                set_val = tmp_df.loc[j, "Value"]

    # Création d'un dictionnaire récupérant les sommes des fréquences 
    # d'apparitions des valeurs de "Value" dans l'ensemble {0, 1, 2, 4}
    sommes_dict ={}
    for keys, values in results_dict.items():
        if sommes_dict.get(keys) is None:
            sommes_dict[keys] = {}
        for vs in values.values():
            if type(vs) == dict:
                for k, v in vs.items():
                    if k == 0:
                        if sommes_dict[keys].get(k) is None:
                            sommes_dict[keys][k] = 0
                        sommes_dict[keys][k] += v
                    elif k == 1:
                        if sommes_dict[keys].get(k) is None:
                            sommes_dict[keys][k] = 0
                        sommes_dict[keys][k] += v
                    elif k == 2:
                        if sommes_dict[keys].get(k) is None:
                            sommes_dict[keys][k] = 0
                        sommes_dict[keys][k] += v
                    elif k == 4:
                        if sommes_dict[keys].get(k) is None:
                            sommes_dict[keys][k] = 0
                        sommes_dict[keys][k] += v

    # Calcul des probabilités
    for keys, values in results_dict.items():
        somme_float = np.sum([v for k, v in values.items() if k in ["InletTemp-sensor", "SetTemp-sensor"]])
        for ks, vs in values.items():
            if ks in ["InletTemp-sensor", "SetTemp-sensor"]:
                results_dict[keys][ks] = np.round(vs / somme_float *100, 3)
            elif ks in ["Drive-sensor", "FanSpeed-sensor", "Mode-sensor"]:
                for k, v in vs.items():
                    results_dict[keys][ks][k] = np.round(v / sommes_dict[keys][k] *100, 3)

    return results_dict

    