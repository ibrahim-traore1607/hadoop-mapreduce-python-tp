#!/usr/bin/env python3
"""reducer_notes.py : moyenne par matière à partir des couples (somme, nombre)."""
import sys

courante, somme, nb = None, 0.0, 0

for ligne in sys.stdin:
    matiere, s, n = ligne.rstrip("\n").split("\t")
    if matiere == courante:
        somme += float(s)
        nb += int(n)
    else:
        if courante is not None:
            print(f"{courante}\t{somme / nb:.2f}")
        courante, somme, nb = matiere, float(s), int(n)

if courante is not None:
    print(f"{courante}\t{somme / nb:.2f}")