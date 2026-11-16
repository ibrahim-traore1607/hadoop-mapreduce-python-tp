#!/usr/bin/env python3
"""mapper_notes.py : émet (matière, note, 1) pour chaque note."""
import sys

for ligne in sys.stdin:
    champs = ligne.strip().split(",")
    if len(champs) != 3:
        continue
    _, matiere, note = champs
    try:
        note = float(note)
    except ValueError:          # ignore un éventuel en-tête
        continue
    print(f"{matiere}\t{note}\t1")