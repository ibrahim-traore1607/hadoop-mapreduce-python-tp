#!/usr/bin/env python3
"""reducer_meteo.py : température maximale par année (entrées triées par année)."""
import sys

annee_courante, maximum = None, None

for ligne in sys.stdin:
    annee, temp = ligne.rstrip("\n").split("\t")
    temp = float(temp)
    if annee == annee_courante:
        if temp > maximum:
            maximum = temp
    else:
        if annee_courante is not None:
            print(f"{annee_courante}\t{maximum}")
        annee_courante, maximum = annee, temp

if annee_courante is not None:
    print(f"{annee_courante}\t{maximum}")