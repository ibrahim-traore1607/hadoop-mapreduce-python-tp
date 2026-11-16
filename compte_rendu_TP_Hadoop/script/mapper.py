#!/usr/bin/env python3
import sys
import re

for ligne in sys.stdin:
   ligne = ligne.strip().lower()
   mots = re.findall(r"[a-zàâäéèêëîïôöùûüç]+", ligne)
   for mot in mots:
      print(f"{mot}\t1")