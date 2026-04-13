# Pipeline BEL Assurance Vie & Analyse de Sensibilité

**Version :** 1.1
**Auteur :** SukHee Lee
**Date :** Avril 2026
**Stack :** dbt + Databricks (Delta Lake)

---

## Résumé

Ce projet implémente un pipeline de projection déterministe de flux de trésorerie pour le calcul du **Best Estimate Liability (BEL)** d'un portefeuille d'assurance vie temporaire non participative, dans le cadre réglementaire **Solvabilité II**.

Il transforme des hypothèses actuarielles figées — tables de mortalité, courbes de rachat, taux d'actualisation et paramètres de frais — en outputs analytiques pour la valorisation réglementaire et l'évaluation des risques :

- **BEL par Cohorte & Scénario** — valeur actuelle des flux futurs nets par model point
- **Analyse de Sensibilité** — réponse du BEL aux stress de mortalité, rachat et taux d'actualisation
- **Cadre de Validation** — preuve automatisée du respect des identités actuarielles
- **Vue d'Ensemble du Portefeuille** — risque agrégé et sensibilité au niveau management

### Utilisateurs des Outputs

| Équipe | Besoin | Modèle MART |
|--------|--------|-------------|
| Actuariat / Valorisation | Composantes BEL, décomposition PV | mart_bel_components_core |
| Gestion des Risques | Sensibilité mortalité, rachat, taux | mart_bel_sensitivity |
| Capital / SCR | Intuition directionnelle SCR par facteur de risque | mart_bel_sensitivity, mart_portfolio_overview |
| Gouvernance / Audit | Preuve d'intégrité des calculs | mart_validation_summary |
| Direction | Résumé BEL et risque au niveau portefeuille | mart_portfolio_overview |

---

## Contexte Métier

### Le Problème

Sous Solvabilité II, les assureurs doivent calculer le **Best Estimate Liability** — la valeur actuelle probable de tous les flux de trésorerie futurs découlant des obligations d'assurance. Pour l'assurance vie, cela implique de projeter les primes, prestations décès et frais mois par mois, d'appliquer les hypothèses de décrément (mortalité, rachat), et d'actualiser au taux sans risque réglementaire.

Le BEL est sensible à ses hypothèses d'entrée. Les régulateurs exigent que les assureurs quantifient cette sensibilité par des tests de stress, qui alimentent le calcul du Capital de Solvabilité Requis (SCR).

### Ce que fait ce Pipeline

1. **Définit** un portefeuille de 8 model points (4 cohortes × 2 sexes) représentant un portefeuille de temporaire décès non participative
2. **Projette** les flux mensuels de la date de valorisation à l'échéance du contrat
3. **Applique** les décréments de mortalité et de rachat pour faire évoluer la population en portefeuille
4. **Actualise** les flux avec la courbe de taux sans risque EIOPA avec interpolation linéaire
5. **Agrège** les valeurs actuelles en BEL par cohorte, sexe et scénario
6. **Stress teste** sous 9 scénarios (mortalité ±10%, rachat ±10%, taux ±50bp, combiné, persistance)
7. **Valide** chaque étape de calcul contre les identités actuarielles

---

## Conception du Portefeuille

| Cohorte | Âge Souscription | Durée | Ancienneté | Âge Atteint | Restant | Objectif Clé |
|---------|-----------------|-------|-----------|-------------|---------|-------------|
| C1 | 35 | 30 ans | 5 ans | 40 | 25 ans | Jeune, longue duration — dominé par les primes, sensible au taux |
| C2 | 50 | 25 ans | 10 ans | 60 | 15 ans | Âge moyen, longue ancienneté — effet de la courbe de rachat |
| C3 | 60 | 20 ans | 5 ans | 65 | 15 ans | Senior — mortalité élevée, même durée restante que C2 pour comparaison |
| C4 | 65 | 15 ans | 3 ans | 68 | 12 ans | Âgé, courte durée restante — dominé par la mortalité |

Chaque cohorte comporte des model points homme et femme avec des nombres de polices différenciés reflétant les tendances démographiques.

---

## Architecture du Pipeline

```
RAW (Databricks Notebook) → STG (dbt view) → INT (dbt view) → VALIDATION (dbt view) → MART (dbt table)
```

| Couche | Modèles | Responsabilité |
|--------|---------|---------------|
| RAW | 8 tables | Hypothèses actuarielles figées & définition produit |
| STG | 8 vues | Standardisation des entrées, filtrage de version, champs dérivés |
| INT | 7 vues | Moteur de projection : taux, roll-forward in-force, flux, actualisation, BEL |
| VALIDATION | 7 vues | Preuve automatisée des identités actuarielles |
| MART | 4 tables | Outputs décisionnels pour consommation métier |
| **Total** | **26 modèles dbt** | |

---

### Lignage des Modèles (DAG)

![DAG](docs/dag.png)

## Décisions Techniques Clés

### 1. Produit Cumulatif pour le Roll-Forward In-Force

Le roll-forward de la population en portefeuille est intrinsèquement récursif : l'ouverture de chaque mois égale la clôture du mois précédent. Au lieu d'une CTE récursive, le pipeline utilise le **pattern de produit cumulatif log-sum-exp** :

```sql
IF_open(t) = policy_count × EXP(SUM(LN((1 - q(k)) × (1 - l(k)))) OVER (... ROWS PRECEDING))
```

Cela produit des résultats mathématiquement identiques sans récursion, s'exécutant efficacement sur Databricks SQL.

### 2. SEQUENCE + EXPLODE pour l'Axe de Projection

Les lignes de projection mensuelle (1 à remaining_months) sont générées dynamiquement avec les fonctions natives Databricks SQL `SEQUENCE()` + `EXPLODE()`.

### 3. Date de Valorisation comme Variable dbt

La date de valorisation est commune à tout le portefeuille. Sa gestion via `{{ var('valuation_date') }}` dans dbt permet une re-valorisation à n'importe quelle date en modifiant un seul paramètre.

### 4. Interpolation Linéaire sur la Courbe d'Actualisation

La courbe RFR EIOPA fournit des points de tenor annuels (12, 24, ..., 1800 mois). Les mois de projection entre les tenors annuels utilisent une interpolation linéaire sur les taux zéro-coupon, avec le shift de stress (bps) appliqué après l'interpolation.

### 5. Taux Négatif sous Stress

Sous RATE_DOWN (-50bp), les taux de base à court terme proches de zéro peuvent produire des taux interpolés négatifs, entraînant un DF > 1,0 au projection_month=1. Ceci est mathématiquement valide et cohérent avec le cadre EIOPA qui autorise les taux négatifs. L'effet est négligeable (diff < 0,001).

---

## Tests de Stress & Résultats

### Conception des Scénarios

| Scénario | Mortalité | Rachat | Taux | Groupe |
|----------|-----------|--------|------|--------|
| BASE | 1,00× | 1,00× | 0 bp | — |
| MORT_UP | 1,10× | 1,00× | 0 bp | Core |
| MORT_DOWN | 0,90× | 1,00× | 0 bp | Core |
| LAPSE_UP | 1,00× | 1,10× | 0 bp | Core |
| LAPSE_DOWN | 1,00× | 0,90× | 0 bp | Core |
| RATE_UP | 1,00× | 1,00× | +50 bp | Core |
| RATE_DOWN | 1,00× | 1,00× | -50 bp | Core |
| COMBINED_ADVERSE | 1,10× | 1,00× | -50 bp | Avancé |
| PERSIST_IMPROVE | 1,00× | 0,70× | 0 bp | Avancé |

### Résultats au Niveau Portefeuille

| Scénario | BEL Total | Delta BEL | Interprétation |
|----------|-----------|-----------|---------------|
| BASE | 18,1M | — | Position nette de passif |
| MORT_UP | +12,4M | +68,5% | La mortalité est le facteur de risque dominant |
| COMBINED_ADVERSE | +14,7M | +80,9% | Effet combiné : mortalité hausse + taux baisse |
| RATE_DOWN | +1,8M | +9,7% | Effet duration sur les flux longs |
| RATE_UP | -1,6M | -9,0% | Impact symétrique de l'actualisation |
| LAPSE_UP | -1,1M | -6,3% | La libération de passif dépasse la perte de primes |

---

## Cadre de Validation

Chaque calcul est validé contre les identités actuarielles :

| Vérification | Ce qu'elle Valide |
|-------------|------------------|
| Continuité de Projection | La séquence mensuelle est continue de 1 à remaining_months |
| Réconciliation In-Force | `ouverture - décès - rachats = clôture` à chaque mois |
| Signe & Timing des Flux | Prime ≤ 0 (début de mois), prestation/frais ≥ 0 (fin de mois) |
| Cohérence de l'Actualisation | DF > 0 et non-null pour tous les flux |
| Réconciliation BEL | `benefit_pv + expense_pv + premium_pv = bel_amount` |
| Direction de Sensibilité | Mortalité hausse → BEL hausse, Taux hausse → BEL baisse |

**Résultat : 72/72 model points × scénarios = TOUS VALIDÉS**

---

## Hypothèses & Sources de Données

| Hypothèse | Source | Granularité |
|-----------|--------|-------------|
| Mortalité | Tables EIOPA illustratives / INSEE / HMD | Âge (0–100) × Sexe |
| Rachat | Courbe déterministe par durée | Année de durée (1–30) |
| Actualisation | Courbe zéro-coupon style EIOPA RFR | Tenor mensuel (12–1800) |
| Frais | Maintenance proportionnelle aux primes (3%) | Taux unique |
| Prime | Formule de tarification Python (figée à la souscription) | Cohorte × Sexe |

Toutes les hypothèses sont générées via Databricks Notebooks (Python) et stockées en tables Delta avec suivi de version.

---

## Stack Technique

- **Databricks** — Notebooks (génération RAW), SQL Warehouse (exécution dbt), Delta Lake (stockage)
- **dbt** (adaptateur dbt-databricks) — Logique de transformation, tests, documentation
- **Delta Lake** — Transactions ACID, contrôle de schéma, time travel
- **Python** — Tarification des primes, génération des hypothèses

---

## Simplifications vs. Production

| Ce Projet | Réalité en Production |
|---|---|
| 8 model points | Millions de polices individuelles ou model points granulaires |
| Projection déterministe | Simulation stochastique pour garanties et options |
| Courbe d'actualisation unique | Courbes multiples (avec/sans VA, MA) |
| Pas de marge de risque | Marge de risque via méthode Coût-du-Capital |
| Pas de calcul SCR | SCR Formule Standard ou Modèle Interne |
| Pas de rachat dynamique | Modèles de comportement assuré liés aux conditions économiques |
| Frais moyens simples | Analyse détaillée des frais par type et allocation |
| Tables de mortalité annuelles | Modèles d'amélioration de mortalité (CMI, Lee-Carter) |
| Pas de réassurance | BEL brut et net avec recouvrement de réassurance |

---

## Série de Projets

Ce projet est la Partie 3 d'une série de 5 projets construisant une plateforme de données d'assurance complète :

1. **Small :** Insurance Policy Admin Mart — Structure de portefeuille & KPIs
2. **Medium-1 :** Motor Insurance Claims Development & Loss Forecasting (P&C, rétrospectif)
3. **Medium-2 :** Ce projet — Pipeline BEL Assurance Vie (Vie, prospectif)
4. **Medium-3 :** Réassurance IFRS 17 — Lien Rétro & Recouvrement de Sinistres (planifié)
5. **Large :** Plateforme Analytics IFRS 17 sur Azure — E2E avec CI/CD + Méthode de Mack (planifié)

---

## Auteur

**SukHee Lee** — Actuarial Data Analyst | IFRS 17 · dbt · Databricks
Construction de pipelines de données d'assurance couvrant le provisionnement, la valorisation et l'analytics engineering.

GitHub : github.com/SHLee5864
Medium : medium.com/@lsh5864
