#!/usr/bin/env python3
"""mapper_index.py : émet (mot, nom_du_fichier) pour chaque mot lu."""
import os
import re
import sys

# Hadoop Streaming expose le fichier en cours de lecture dans cette variable
chemin = os.environ.get("mapreduce_map_input_file") or os.environ.get("map_input_file", "inconnu")
fichier = os.path.basename(chemin)      # on garde seulement le nom, pas le chemin complet

for ligne in sys.stdin:
    mots = re.findall(r"[a-zàâäéèêëîïôöùûüç]+", ligne.strip().lower())
    for mot in mots:
        print(f"{mot}\t{fichier}")