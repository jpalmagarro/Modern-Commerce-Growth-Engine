# Modern Commerce Growth Engine (MCGE)

**Project Role:** Analytics Engineer & Business Partner
**Client:** Olist (Brazilian E-Commerce)
**Objective:** Unify Backend Data (Transactions) with Frontend Data (Digital Footprint) to unlock Growth Accounting.

---

## 1. El Problema (The Pain)
Olist tiene una visibilidad perfecta de **qué** se vende (Backend), pero es ciega respecto a:
*   **Customer Journey:** ¿Qué hacen los usuarios antes de comprar?
*   **Efficiency:** ¿Qué canales de marketing traen clientes con alto LTV vs. solo tráfico barato?
*   **Experimentation:** No existe infraestructura para medir el impacto real de nuevos features (A/B Testing).

## 2. La Solución (The Build)
Hemos construido una arquitectura **Modern Data Stack** completa:

1.  **Ingestion & Generation (Python):**
    *   Simulación de 1M+ de eventos de navegación (`web_events`) correlacionados con transacciones reales.
    *   Generación de datos de Marketing Spend y A/B Testing Groups.
2.  **Storage (Snowflake / Data Warehouse):**
    *   Centralización de datos Raw (Olist + Synthetic).
3.  **Transformation (dbt Core):**
    *   **Sessionization:** Algoritmo SQL complejo para reconstruir sesiones de usuario basado en tiempos de inactividad (30 min window).
    *   **Attribution:** Modelado dimensional para asignar Conversiones a Canales (First Touch / Last Touch).
    *   **Business Logic:** Cálculo de ROAS, CR (Conversion Rate) y A/B Significance.

## 3. Resultados Clave (Insights)
*   **Hallazgo 1**: El "Nuevo Checkout" incrementa la conversión en un **0.4%** globalmente, pero degrada la experiencia en Mobile debido a tiempos de carga.
*   **Hallazgo 2**: El canal 'Paid Social' tiene un CAC bajo, pero un LTV un **40% menor** que 'Organic Search'.
*   **Recomendación**: Redistribuir presupuesto de FB Ads hacia SEO y Content Marketing.

---

## Estructura del Repositorio

```bash
├── dbt_project/       # Transformaciones SQL (The Core)
│   ├── models/        # Marts, Intermediate, Staging
│   ├── tests/         # Data Quality Gates
│   └── macros/        # DRY SQL Functions
├── scripts/           # Python Data Generation (The Engine)
└── data/              # Raw Data (Gitignored in production)
```
