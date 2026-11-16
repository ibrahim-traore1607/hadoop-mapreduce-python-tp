#!/usr/bin/env python3

import csv
import random

# Nombre d'enregistrements à générer
N = 100

etudiants = [
    "Sanogo", "kossonou", "Jean", "Traore",
    "Fabrice", "Die", "Aristide", "Ibrahim",
    "Bakary", "Segue"
]

matieres = [
    "BigData", "cloud", "Merise", "RechercheOperationnelle",
    "Anglais", "BaseDonnees","UML"
]

# Génération du fichier CSV
with open("notes.csv", "w", newline="", encoding="utf-8") as fichier:

    writer = csv.writer(fichier)

    # En-tête
    writer.writerow(["etudiant", "matiere", "note"])