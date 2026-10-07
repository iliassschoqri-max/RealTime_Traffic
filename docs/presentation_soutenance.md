# Présentation Soutenance PFA — Pipeline Trafic Routier Temps Réel

## Structure recommandée (15-20 minutes)

---

## Slide 1 : Page de Garde

```markdown
# PFA — Pipeline Data Engineering
## Analyse du Trafic Routier en Temps Réel
### Ville de Tanger

**Équipe :**
- Youssef MOUHI — Lead Data Engineer
- Malak ARIBI — Spark & Delta Lake
- Ilias COUKRI — Kafka & Infrastructure
- Omaima AMTOT — Hive & Modélisation Gold
- Soufian TAZI CHERTI — Power BI & Visualisation

**Encadrant :** Radouan DAHBI
**EMSI — Data Science / IA — 2025/2026**
```

---

## Slide 2 : Problématique & Objectifs

```mermaid
mindmap
  root((Problématique))
    Mobilité Urbaine
      Congestion croissante à Tanger
      Impacts économiques et environnementaux
      Nécessité de données temps réel
    Défis Techniques
      Collecter des flux en continu
      Transformer et nettoyer en streaming
      Stocker de manière fiable (ACID)
      Exposer via SQL distribué
      Visualiser et analyser
    Objectifs
      Pipeline end-to-end Kafka → Visualisation
      Architecture Medallion (Bronze/Silver/Gold)
      Modèles ML embarqués
      Dashboard temps réel
      Rapports automatisés
```

**Message clé :** Construire un système industriel complet de la donnée à la visualisation.

---

## Slide 3 : Architecture Technique Globale

```mermaid
graph TB
    P[Python Producer<br/>Tanger] -->|JSON| K[Apache Kafka<br/>traffic-events]
    K --> B[PySpark Bronze<br/>Raw Data]
    B --> S[PySpark Silver<br/>Clean + Enrich]
    S --> G[PySpark Gold<br/>Star Schema]
    G --> H[Hive Metastore]
    G --> T[Trino SQL]
    T --> API[FastAPI KPI API]
    T --> ML[FastAPI ML Server]
    T --> PBI[Power BI]
    API --> DASH[Dashboard Web]
    ML --> DASH
```

**Architecture Medallion (Bronze → Silver → Gold)** + **Cache-First API** + **5 modèles ML**.

---

## Slide 4 : Composant 1 — Producteur & Kafka

```mermaid
graph LR
    subgraph "🟢 Producteur Python"
        G1[generate_clean_event<br/>70% du temps]
        G2[generate_dirty_event<br/>30% du temps]
    end

    G1 --> TOPIC
    G2 --> TOPIC

    subgraph "🟡 Kafka topic: traffic-events"
        TOPIC(("3 partitions<br/>Broker:9092<br/>Compression LZ4"))
    end

    subgraph "Data Quality Challenge"
        D1["9 types d'anomalies"]
        D2["null_speed, negative_speed"]
        D3["duplicate_vehicle, future_event"]
        D4["wrong_datatype, corrupt_json"]
    end

    TOPIC --> SPARK[PySpark Streaming]
    SPARK --> D1
    D1 --> D2
    D2 --> D3
    D3 --> D4
```

**Points clés :**
- 8 axes routiers de Tanger modélisés
- 30% de données sales pour tester la résilience
- Simulation des heures de pointe (6-9h, 16-19h)

---

## Slide 5 : Composant 2 — Pipeline Streaming PySpark

```mermaid
graph TB
    subgraph "Bronze"
        B1[readStream Kafka]
        B2[Parse JSON → BRONZE_SCHEMA]
        B3[Métadonnées: ingestion_time, version]
        B1 --> B2 --> B3
    end

    subgraph "Silver"
        S1[Quality Checks<br/>Nulls, Speed [0,200], Type]
        S2[Déduplication<br/>watermark 10s]
        S3[Enrichissement<br/>speed_category, time_of_day,<br/>is_incident, is_congestion]
        S1 --> S2 --> S3
    end

    subgraph "Gold"
        G1[dim_time]
        G2[dim_road_segment]
        G3[dim_event_type]
        G4[dim_vehicle]
        G5[fact_traffic_events]
        G6[ThreadPoolExecutor<br/>5 workers parallèles]
        G1 --> G6
        G2 --> G6
        G3 --> G6
        G4 --> G6
        G5 --> G6
    end

    B3 --> S1
    S3 --> G1
    S3 --> G2
    S3 --> G3
    S3 --> G4
    S3 --> G5
```

**Points clés :**
- **Bronze :** 100% des données brutes préservées
- **Silver :** ~30% filtrées (données sales intentionnelles)
- **Gold :** Star Schema optimisé pour BI

---

## Slide 6 : Composant 3 — Infrastructure Docker

```mermaid
graph TB
    subgraph "🐳 Docker Compose — 6 Services"
        Z[Zookeeper<br/>2181]
        K[Kafka<br/>9092]
        UI[Kafka UI<br/>8080]
        PG[(PostgreSQL<br/>5432)]
        HM[Hive Metastore<br/>9083]
        TR[Trino<br/>8081]
    end

    Z --> K
    K --> UI
    PG --> HM
    HM --> TR
    K -->|"readStream"| BRONZE

    subgraph "Host Volumes"
        D["./data/delta → /data/delta:ro"]
        C["./conf → /opt/hive/custom-conf"]
    end

    D --> HM
    D --> TR
    C --> HM
```

**Points clés :**
- Infrastructure conteneurisée complète (1 commande : `docker-compose up -d`)
- Hive Metastore → Catalogue de tables pour Trino
- Trino → Moteur SQL distribué sur les fichiers Delta

---

## Slide 7 : Composant 4 — API Cache-First

```mermaid
sequenceDiagram
    participant U as Utilisateur
    participant API as FastAPI
    participant C as STATE_CACHE
    participant W as Worker
    participant T as Trino

    Note over API,W: Démarrage
    API->>T: Premier refresh synchrone
    T-->>API: Snapshot complet
    API->>C: STATE_CACHE = snapshot
    API->>W: Lancement background loop

    loop Toutes les 5s
        W->>T: 5 requêtes parallèles
        T-->>W: Nouveaux KPIs
        W->>C: Update STATE_CACHE
        W->>U: WebSocket push UPDATE_DATA
    end

    U->>API: GET /api/kpi/summary
    API->>C: Lecture O(1)
    C-->>API: {total_events, avg_speed...}
    API-->>U: ⚡ Réponse instantanée
```

**Message clé :** Zéro requête Trino au clic utilisateur. Cache rafraîchi toutes les 5s.

---

## Slide 8 : Composant 5 — Machine Learning

```mermaid
graph TB
    subgraph "Trino"
        T[(Delta Gold<br/>fact_traffic_events)]
    end

    subgraph "ML Server — 5 Modèles"
        M1["🌲 Prédiction Congestion<br/>RandomForest Regressor<br/>MAE ~8 km/h"]
        M2["🔍 Détection Anomalies<br/>Isolation Forest<br/>contamination 8%"]
        M3["📈 Prévision Segment<br/>Linear Regression<br/>hour_sin/cos"]
        M4["⚠️ Classification Sévérité<br/>RandomForest Classifier<br/>NORMAL/MODÉRÉ/CRITIQUE"]
        M5["🛣️ Recommandation Itinéraire<br/>Multi-Factor Scoring<br/>5 meilleures alternatives"]
    end

    subgraph "Cache + API"
        C[DataCache TTL=30s]
        WS[WebSocket push toutes les 10s]
    end

    T --> M1
    T --> M2
    T --> M3
    T --> M4
    T --> M5

    M1 --> C
    M2 --> C
    M3 --> C
    M4 --> C
    M5 --> C

    C --> WS
```

**Message clé :** Intelligence embarquée avec rafraîchissement automatique.

---

## Slide 9 : Résultats — KPIs & Métriques

```mermaid
graph LR
    subgraph "📊 KPIs Temps Réel"
        K1["Total Événements<br/>fact_traffic_events"]
        K2["Vitesse Moyenne<br/>AVG(speed_kmh)"]
        K3["Taux Congestion<br/>% is_congestion=1"]
        K4["Taux Incidents<br/>% is_incident=1"]
        K5["Segments Actifs<br/>DISTINCT segment_key"]
        K6["Véhicules Uniques<br/>DISTINCT vehicle_id"]
    end

    K1 --> API["GET /api/kpi/summary"]
    K2 --> API
    K3 --> API
    K4 --> API
    K5 --> API
    K6 --> API
```

**Métriques clés :**
| Métrique | Valeur |
|---|---|
| Débit ingestion | ~1 event/s (scalable) |
| Latence bout en bout | < 5s |
| Taux données sales filtrées | ~30% |
| Temps de refresh cache | 5s (API), 10s (ML) |
| Nombre de modèles ML | 5 |

---

## Slide 10 : Pipeline d'Audit — Traçabilité

```mermaid
graph TB
    subgraph "🕵️ Audit Pipeline — Preuve de Traçabilité"
        K("Kafka<br/>(messages bruts)")
        B("Bronze<br/>(Delta Lake)")
        S("Silver<br/>(après filtres)")
        G("Gold Fact<br/>(Star Schema)")
    end

    K -->|"Snapshot batch"| B
    B -->|"- Rejet NULL<br/>- Rejet vitesse [0,200]<br/>- Rejet type invalide"| S
    S -->|"- Déduplication<br/>vehicle_id + timestamp"| G

    K -.->|"DeltaTable.history()<br/>→ Logs de transactions<br/>→ Preuve forensique"| G
    
    T["Delta Transaction Log<br/>version, timestamp,<br/>operation, numOutputRows"]
    G -.-> T
```

**Message clé :** Chaque écart Kafka → Gold est **expliqué et justifié**. Les logs Delta Lake constituent une preuve irréfutable.

---

## Slide 11 : Visualisation — Dashboard & Power BI

```mermaid
graph TB
    subgraph "📱 Dashboard Web (FastAPI Static)"
        W1["KPIs en temps réel<br/>Total events, avg speed"]
        W2["Carte géospatiale<br/>Segments routiers"]
        W3["Timeline 24h<br/>Évolution vitesse"]
        W4["ML Insights<br/>Anomalies, Prédictions"]
        W5["WebSocket Push<br/>Mise à jour 5s"]
    end

    subgraph "📊 Power BI"
        P1["Connexion Trino ODBC<br/>host: localhost:8081"]
        P2["Tables disponibles<br/>delta.gold.*"]
        P3["Star Schema<br/>Faits + 4 Dimensions"]
        P4["Rapports interactifs<br/>Filtres, slicers"]
    end

    API[FastAPI] --> W1
    API --> W2
    API --> W3
    API --> W4
    API --> W5

    TR[Trino :8081] --> P1
    P1 --> P2
    P2 --> P3
    P3 --> P4
```

---

## Slide 12 : Démonstration en Direct

```bash
# === Étape 1 : Vérifier l'infrastructure ===
docker ps
# Attendu : 6 containers en cours d'exécution

# === Étape 2 : Vérifier Kafka ===
docker exec pfa_kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
# → traffic-events

# === Étape 3 : Compter les messages dans Gold ===
docker exec pfa_trino trino \
  --execute "SELECT COUNT(*) FROM delta.gold.fact_traffic_events"

# === Étape 4 : KPIs agrégés ===
docker exec pfa_trino trino \
  --execute "SELECT COUNT(*) AS total_events,
    ROUND(AVG(speed_kmh), 1) AS avg_speed,
    ROUND(AVG(is_congestion)*100, 1) AS congestion_pct
  FROM delta.gold.fact_traffic_events"

# === Étape 5 : Vérifier l'API ===
curl http://localhost:8000/api/kpi/summary

# === Étape 6 : Ouvrir le dashboard ===
start http://localhost:8000
```

---

## Slide 13 : Bilan & Perspectives

```mermaid
graph TB
    subgraph "✅ Ce qui a été réalisé"
        A1["Pipeline end-to-end Kafka → Gold"]
        A2["Architecture Medallion (Bronze/Silver/Gold)"]
        A3["Star Schema optimisé BI"]
        A4["5 modèles ML opérationnels"]
        A5["API Cache-First (latence zéro)"]
        A6["Pipeline d'audit et traçabilité"]
        A7["Dashboard temps réel + Power BI"]
        A8["Rapports PDF/CSV automatisés"]
    end

    subgraph "🚀 Perspectives"
        B1["Données réelles (API trafic)"]
        B2["Déploiement Cloud (AWS/GCP)"]
        B3["Deep Learning (LSTM)"]
        B4["CI/CD + Tests automatisés"]
        B5["Monitoring Prometheus/Grafana"]
        B6["Schema Registry (Avro)"]
        B7["Tests de charge à grande échelle"]
    end
```

---

## Slide 14 : Compétences Acquises

| Domaine | Compétences |
|---|---|
| **Data Engineering** | Kafka, PySpark, Delta Lake, Hive, Trino |
| **Streaming** | Structured Streaming, watermark, déduplication |
| **Architecture** | Medallion, Star Schema, Cache-First |
| **DevOps** | Docker, Docker Compose |
| **API** | FastAPI, WebSocket, REST |
| **Machine Learning** | RandomForest, Isolation Forest, Linear Regression |
| **BI** | Power BI, Trino ODBC |
| **Qualité** | Data Quality, Audit, Traçabilité |

---

## Slide 15 : Questions / Remerciements

```markdown
# Merci pour votre attention !

## Questions ?

### Liens utiles
- **GitHub :** https://github.com/...
- **Dépôt du projet :** ...

### Démonstration en direct
```

---

## Conseils pour la Soutenance

| Minute | Slide | Action |
|---|---|---|
| 0-2 | 1-2 | Introduction, contexte, problématique |
| 2-5 | 3 | Architecture globale (vue d'ensemble) |
| 5-8 | 4-5 | Pipeline détaillé (Producer → Gold) |
| 8-10 | 6 | Infrastructure Docker |
| 10-12 | 7-8 | API + ML |
| 12-14 | 9-10 | Résultats + Audit |
| 14-15 | 11 | Visualisation |
| 15-17 | 12 | Démo en direct |
| 17-18 | 13-14 | Bilan, perspectives, compétences |
| 18-20 | 15 | Questions |
