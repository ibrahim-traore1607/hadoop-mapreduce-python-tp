#!/usr/bin/env python3
"""combiner_notes.py : (matière, somme, nombre) -> (matière, somme, nombre)."""
import sys

courante, somme, nb = None, 0.0, 0

for ligne in sys.stdin:
    matiere, s, n = ligne.rstrip("\n").split("\t")
    if matiere == courante:
        somme += float(s)
        nb += int(n)
    else:
        if courante is not None:
            print(f"{courante}\t{somme}\t{nb}")
        courante, somme, nb = matiere, float(s), int(n)

if courante is not None:
    print(f"{courante}\t{somme}\t{nb}")