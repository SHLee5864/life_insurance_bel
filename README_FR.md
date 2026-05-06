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

## 📈 Analyse Multi-Courbes EIOPA RFR

Le pipeline supporte simultanément plusieurs courbes d'actualisation EIOPA RFR, permettant la comparaison du BEL entre versions de courbes.

### Courbes Chargées

| Version ID | Date | VA | Source |
|---|---|---|---|
| RFR_20251231_noVA | 2025-12-31 | Non | EIOPA Monthly RFR |
| RFR_20251231_withVA | 2025-12-31 | Oui | EIOPA Monthly RFR |
| RFR_20260331_noVA | 2026-03-31 | Non | EIOPA Monthly RFR |
| RFR_20260331_withVA | 2026-03-31 | Oui | EIOPA Monthly RFR |

### Impact BEL (Scénario BASE)

| Courbe | BEL Total | Δ vs 2025-Q4 noVA |
|---|---|---|
| 2025-Q4 sans VA | 18,14M€ | — |
| 2025-Q4 avec VA | 17,67M€ | −2,6% |
| 2026-Q1 sans VA | 18,04M€ | −0,6% |
| 2026-Q1 avec VA | 17,44M€ | −3,9% |

**Écart maximum : 0,70M€ (3,9%)** — entièrement dû au choix de la courbe d'actualisation.

### Implémentation

L'ajout de quatre courbes a nécessité la modification de 9 modèles dbt sans aucun changement de logique actuarielle :
- `stg_discount_curve` — suppression du filtre mono-version, exposition de toutes les versions de courbes
- `int_cashflows_discounted` — cross join avec les versions de courbes pour actualisation parallèle
- Modèles en aval — propagation du `version_id` dans les GROUP BY et conditions de JOIN

Cela a fonctionné car les courbes d'actualisation étaient modélisées comme des données (hypothèses versionnées), et non comme de la logique.

Voir [Article Medium #10 : Discounting Is Not a Number](https://medium.com/@lsh5864) pour l'analyse complète.

---

## 📊 Analyse Multi-Versions Mortalité

Le pipeline supporte simultanément plusieurs versions d'hypothèses de mortalité, permettant la comparaison du BEL entre différentes bases de mortalité.

### Versions de Mortalité Chargées

| Version ID | Source | Méthode |
|---|---|---|
| MORT_2026_04 (INSEE 2019) | Table de mortalité nationale INSEE, 2019 | Référence industrielle, instantané mono-année |
| MORT_EXP_STUDY | HMD France 2015–2023 | Étude d'expérience : graduation A/E, pondération de crédibilité |

La mortalité issue de l'étude d'expérience a été construite selon le processus actuariel standard : calcul du ratio A/E, graduation polynomiale et pondération de crédibilité par fluctuation limitée contre la table industrielle INSEE 2019. La fenêtre d'observation de neuf ans (2015–2023) inclut les années de pandémie COVID-19, produisant une mortalité structurellement plus élevée aux âges avancés.

### Impact BEL (Scénario BASE, Courbe 2025-Q4 sans VA)

| Version Mortalité | BEL Total | Δ vs INSEE 2019 |
|---|---|---|
| INSEE 2019 | 18,14M€ | — |
| Étude d'Expérience | 26,83M€ | +8,7M€ (+48%) |

**Effet mortalité : +48%.** Effet courbe d'actualisation sur les quatre courbes : 3,9%. Dans ce portefeuille, la mortalité domine l'actualisation comme facteur déterminant du BEL d'un ordre de grandeur.

### Effet Croisé : Mortalité × Actualisation

L'impact VA de l'actualisation augmente sous une mortalité plus élevée (−0,47M€ avec INSEE vs −0,56M€ avec Étude d'Expérience), confirmant que mortalité et actualisation ne sont pas indépendantes — elles interagissent à travers la structure des flux.

### Implémentation

L'ajout d'une seconde version de mortalité a nécessité la modification de 12 modèles dbt sans aucun changement de logique actuarielle — le même pattern d'extension dimensionnelle démontré avec l'actualisation multi-courbes (Article #10). La colonne `mort_version_id` a été propagée dans les GROUP BY et conditions de JOIN aux côtés du `version_id` existant.

Voir [Article Medium #11 : Assumptions Are Built, Not Given](https://medium.com/@lsh5864) pour la méthodologie complète de l'étude d'expérience et l'analyse d'impact BEL.

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
| Courbe d'actualisation unique | Multi-courbes implémentées (4 versions EIOPA RFR) |
| Pas de marge de risque | Marge de risque via méthode Coût-du-Capital |
| Pas de calcul SCR | SCR Formule Standard ou Modèle Interne |
| Pas de rachat dynamique | Modèles de comportement assuré liés aux conditions économiques |
| Frais moyens simples | Analyse détaillée des frais par type et allocation |
| Tables de mortalité annuelles | Multi-versions implémentées (INSEE + Étude d'Expérience) ; en production, ajout de modèles d'amélioration de mortalité (CMI, Lee-Carter) |
| Pas de réassurance | BEL brut et net avec recouvrement de réassurance |

---

## Série de Projets

Ce projet est la Partie 3 d'une série de 5 projets construisant une plateforme de données d'assurance complète :

1. **Small :** Insurance Policy Admin Mart — Structure de portefeuille & KPIs
2. **Medium-1 :** Motor Insurance Claims Development & Loss Forecasting (P&C, rétrospectif)
3. **Medium-2 :** Ce projet — Pipeline BEL Assurance Vie (Vie, prospectif)
4. **Medium-3:** Reinsurance IFRS 17 — Retro Linkage & Loss Recovery
5. **Large:** Insurance Fraud Detection Pipeline on Azure (planned)

---

## Auteur

**SukHee Lee** — Actuarial Data Analyst | IFRS 17 · dbt · Databricks
Construction de pipelines de données d'assurance couvrant le provisionnement, la valorisation et l'analytics engineering.

GitHub : github.com/SHLee5864
Medium : medium.com/@lsh5864
