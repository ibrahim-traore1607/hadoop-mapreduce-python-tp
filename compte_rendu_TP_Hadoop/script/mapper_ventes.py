#!/usr/bin/env python3

import sys
for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne or ligne.startswith("date"):
        continue
    champs = ligne.split(",")
    if len(champs) != 5:
        continue
    _, ville, _, quantite, prix = champs
    try:
        montant = int(quantite) * float(prix)
    except ValueError:
        continue
    print(f"{ville}\t{montant}")