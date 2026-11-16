# TP Hadoop — MapReduce en Python (Hadoop Streaming)

![Hadoop](https://img.shields.io/badge/Hadoop-3.5.0-66CCFF?logo=apachehadoop&logoColor=white)
![Python](https://img.shields.io/badge/Python-3-3776AB?logo=python&logoColor=white)
![Java](https://img.shields.io/badge/Java-17-ED8B00?logo=openjdk&logoColor=white)
![Ubuntu](https://img.shields.io/badge/Ubuntu-26.04%20LTS-E95420?logo=ubuntu&logoColor=white)

Travaux pratiques de **Big Data** : installation d'un cluster **Hadoop 3.5.0 pseudo-distribué** (HDFS + YARN) sous Ubuntu, puis programmation de traitements **MapReduce en Python** avec **Hadoop Streaming** (WordCount, agrégations, combiner, index inversé).

> Cours : Introduction au Big Data · L3 SI BDGL — Professeur : Dr. Bakayoko

---

## Sommaire

- [Objectifs](#objectifs)
- [Environnement](#environnement)
- [Structure du dépôt](#structure-du-dépôt)
- [Exercices](#exercices)
- [Démarrage rapide](#démarrage-rapide)
- [Résultats principaux](#résultats-principaux)
- [Compte rendu](#compte-rendu)
- [Limites et état d'avancement](#limites-et-état-davancement)
- [Auteur](#auteur)

---

## Objectifs

- Installer et configurer un cluster Hadoop (HDFS et YARN) sur une machine virtuelle.
- Manipuler HDFS : blocs, réplication, `fsck`, explorateur web.
- Écrire des programmes **map** et **reduce** en Python et les exécuter avec Hadoop Streaming.
- Comprendre le *shuffle & sort*, le rôle du **combiner**, le partitionnement et le nombre de reducers.
- Lire les compteurs d'un job (YARN et JobHistory Server) pour mesurer l'effet des optimisations.

## Environnement

| Élément | Version utilisée |
| --- | --- |
| Système | Ubuntu Server 26.04.1 LTS (VM VirtualBox, réseau NAT) |
| Hadoop | 3.5.0 (mode pseudo-distribué : 1 NameNode, 1 DataNode) |
| Java | OpenJDK 17 (exigé par Hadoop 3.5.0) |
| Python | 3 |

Interfaces web : NameNode `http://localhost:9870`, ResourceManager `http://localhost:8088`, JobHistory Server `http://localhost:19888`.

## Structure du dépôt

```text
.
├── README.md
├── compte_rendu/
│   ├── TP_hadoop.md              # compte rendu complet
│   └── captures/                 # captures d'écran du compte rendu
├── scripts/                      # mappers, combiner et reducers Python
│   ├── mapper.py / reducer.py                  # WordCount
│   ├── mapper_ventes.py / reducer_ventes.py    # chiffre d'affaires par ville
│   ├── mapper_meteo.py / reducer_meteo.py      # température maximale par année
│   ├── generation.py                           # génère notes.csv
│   ├── mapper_notes.py / combiner_notes.py / reducer_notes.py   # moyenne par matière
│   └── mapper_index.py / reducer_index.py      # index inversé

```

## Exercices

| # | Exercice | Clé du résultat | Combiner |
| --- | --- | --- | --- |
| 1 | Manipuler HDFS (`put`, `ls`, `cat`, `fsck`) | — | — |
| 2 | **WordCount** (poème, puis *Les Misérables*) | le mot | oui (somme) |
| 3 | **Chiffre d'affaires** et nombre de ventes par ville | la ville | non (le format change) |
| 4 | **Température maximale** par année | l'année | oui (maximum) |
| 5 | **Moyenne des notes** par matière | la matière | oui, via des couples `(somme, nombre)` |
| 6 | **Index inversé** : pour chaque mot, les fichiers qui le contiennent | le mot | non |
| — | Plusieurs reducers (`mapreduce.job.reduces=3`) | — | — |

## Démarrage rapide

### 1. Démarrer le cluster

```bash
start-dfs.sh
start-yarn.sh
mapred --daemon start historyserver
jps        # NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```

> `hdfs namenode -format` ne se lance qu'**une seule fois** (à l'installation) : il efface l'état de HDFS.

### 2. Tester un programme en local (sans Hadoop)

Le tube `cat | mapper | sort | reducer` simule un job MapReduce, `sort` jouant le rôle du *shuffle & sort* :

```bash
cd scripts
chmod +x *.py
cat ../data/poeme.txt | ./mapper.py | sort | ./reducer.py
```

### 3. Lancer le job sur le cluster

```bash
hdfs dfs -mkdir -p /user/hadoop/tp/input
hdfs dfs -put -f ../data/poeme.txt /user/hadoop/tp/input/

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input /user/hadoop/tp/input -output /user/hadoop/tp/output_wordcount

hdfs dfs -cat /user/hadoop/tp/output_wordcount/part-00000
```

Points d'attention :

- Le dossier de sortie ne doit **pas exister** : le supprimer avant de relancer avec `hdfs dfs -rm -r -f <sortie>`.
- Lancer la commande depuis le dossier qui contient les scripts (`-files` utilise des chemins relatifs).
- Écrire la commande `hadoop jar` sur **une seule ligne** (ou vérifier les `\` de fin de ligne).

### 4. Ajouter un combiner ou plusieurs reducers

```bash
# combiner (WordCount)
... -mapper "python3 mapper.py" -combiner "python3 reducer.py" -reducer "python3 reducer.py" ...

# 3 reducers : l'option -D se place juste après le .jar
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar -D mapreduce.job.reduces=3 ...
```

## Résultats principaux

**Effet du combiner** (compteur `Reduce input records`, lu dans l'historique des jobs) :

| Traitement | Sans combiner | Avec combiner | Réduction |
| --- | --- | --- | --- |
| WordCount sur *Les Misérables* | 122 572 | 16 492 | −86,5 % |
| Moyenne des notes (100 notes) | 100 | 14 | −86 % |

**Chiffre d'affaires par ville** (exercice 3) :

| Ville | CA total | Ventes |
| --- | --- | --- |
| Abidjan | 910 000 | 3 |
| Bouake | 445 000 | 2 |
| Korhogo | 40 000 | 1 |
| San-Pedro | 260 000 | 2 |
| Yamoussoukro | 540 000 | 2 |

**Autres résultats :**

- *Les Misérables* : 12 594 mots distincts ; les plus fréquents sont `de` (4 472), `il` (3 192), `la` (3 040), `et` (2 949) et `le` (2 548).
- **3 reducers** : 3 fichiers de sortie de 45 057, 46 553 et 46 523 octets, soit 138 133 octets au total ; chaque mot est dans un seul fichier, sans ordre global entre les fichiers.
- **Moyenne des notes** : `notes.csv` est généré aléatoirement par `generation.py`. Les moyennes changent à chaque génération ; utiliser le fichier du dépôt pour reproduire les valeurs du compte rendu.
- **Index inversé** : 299 mots distincts sur trois extraits des *Misérables* (les 10 premières lignes des chapitres 1, 2 et 3).

## Compte rendu

Le compte rendu détaillé (commandes, code commenté ligne par ligne, résultats, captures et réponses aux questions de synthèse) se trouve dans [`compte_rendu/TP_hadoop.md`](compte_rendu/TP_hadoop.md). Les images sont lues dans `compte_rendu/captures/` : le dossier doit rester à côté du fichier `.md`.

## Limites et état d'avancement

- Les jobs Hadoop des exercices **4** (météo) et **6** (index inversé) restent à exécuter sur le cluster : le compte rendu donne leur code, testé en local avec le tube `cat | mapper | sort | reducer`, et leur résultat attendu.
- L'index inversé testé en local avec le `sort` du terminal peut afficher un mot accentué (`à`) en deux lignes : le `sort` dépend de la langue du système. Le tri par octets de Hadoop regroupe chaque clé.
- Le motif du mapper ne conserve pas l'apostrophe : `l'homme` est découpé en `l` et `homme` (d'où des « mots » comme `l`, `d` ou `qu` dans les résultats).

## Auteur

**TRAORE IBRAHIM** — L3 SI BDGL
