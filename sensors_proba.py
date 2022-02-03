"""Programme destiné à afficher la probabilité d'apparition d'un capteur
en fonction de la valeur précédente et de l'historique."""


import pandas as pd
import numpy as np
from sensors_db_connection import db_connection
from sensors_datas_cleaning import datas_formatting
from sensors_proba_analysis import proba_analysis
from pprint import pprint



if __name__ == "__main__":
    # Connexion à la base de données et extraction des tables.
    # La fonction db_connection() se situe dans le fichier "sensors_db_connection.py"
    measures_df, sensors_df, engine = db_connection()

    # Mise en forme du jeu de données
    # La fonction datas_formatting() se trouve dans le fichier 
    # "sensors_datas_cleaning.py"
    cleaned_datas, values_dict = datas_formatting(measures_df, sensors_df)
    
    # Analyse des probabilités d'apparition selon le type de données
    # La fonction proba_analysis() se situe dans le fichier "sensors_proba_analysis.py"
    proba_dict = proba_analysis(cleaned_datas, values_dict)

    print()
    pprint(proba_dict)
    print(f"Le dictionnaire ci-dessus se lit comme suit:\nSi le capteur précédant est "
        f"'premier capteur' et que la valeur d'entrée est 'valeur' alors la probabilité "
        f"que ce soit 'deuxième capteur' est de 'pourcentage'%.")
    print(f"Par exemple:\nSi le capteur précédent est 'Drive-sensor' que la valeur d'entrée "
        f"est '1' alors la probabilité que ce soit 'Drive-sensor' est de '3.025'%.")

    # Boucle demandant une nouvelle valeur à tester à chaque tour et affichant les 
    # probabilités d'apparitions
    while True:

        valeur = input(f"Entrer une valeur à tester ('q' pour quitter):\n")

        if valeur.lower() == "q":
            break

        # Conversion de la valeur entrée
        try:
            valeur = np.float64(valeur)
        except:
            print(f"Entrer une valeur numérique.")
            continue

        # Vérification si la valeur entrée est dans l'intervalle [16, 32] ou 
        # dans l'ensemble {0, 1, 2, 4}
        if 16 <= valeur <= 32 or valeur in [0, 1, 2, 4]:
            # Affiche des probabilités par équipement
            print(f"Selon l'historique des données, les probabilités sont les suivantes:")
            # Parcours du jeu de données équipement par équipement
            for i in range(1, 20):
                tmp_df = cleaned_datas[cleaned_datas["EquipmentId"] == i].reset_index(drop= True)

                print(f"\nÉquipement {i}:")
                # Récupération du capteur du dernier relevé
                prev_capt = tmp_df.loc[len(tmp_df) -1, "Name"]

                # Extraction du premier rang de probabilité
                tmp_proba = proba_dict[prev_capt]
                
                print_bool = False
                # Si "valeur" est dans l'invervalle [16, 32]
                if 16 <= valeur <= 32:
                    for k, v in tmp_proba.items():
                        if type(v) == np.float64:
                            print(f"{v}% que ce soit le capteur {k}")
                            print_bool = True
                else:
                    for key, value in tmp_proba.items():
                        if type(value) == dict:
                            for k, v in value.items():
                                if valeur == k:
                                    print(f"{v}% que ce soit le capteur {key}")
                                    print_bool = True

                if not print_bool:
                    print(f"Cas inconnu d'après l'historique des données.")
        else:
            print(f"{valeur} n'est pas acceptée.")

