#!/usr/bin/env python3
"""mapper_meteo.py : émet (année, température) pour chaque mesure."""
import sys

for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne:
        continue
    champs = ligne.split(",")
    if len(champs) != 3:          # format attendu : annee,station,temperature
        continue
    annee, _, temp = champs       # la station n'est pas utilisée
    try:
        temp = float(temp)
    except ValueError:            # ignore les lignes invalides (y compris un en-tête)
        continue
    print(f"{annee}\t{temp}")