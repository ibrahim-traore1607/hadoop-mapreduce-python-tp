#!/usr/bin/env python3
"""reducer_index.py : pour chaque mot, la liste triée sans doublon des fichiers."""
import sys

def emettre(mot, fichiers):
    print(f"{mot}\t{', '.join(sorted(fichiers))}")

mot_courant, fichiers = None, set()

for ligne in sys.stdin:
    mot, fichier = ligne.rstrip("\n").split("\t", 1)
    if mot == mot_courant:
        fichiers.add(fichier)
    else:
        if mot_courant is not None:
            emettre(mot_courant, fichiers)
        mot_courant, fichiers = mot, {fichier}

if mot_courant is not None:
    emettre(mot_courant, fichiers)