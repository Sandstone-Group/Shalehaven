# Shalehaven Scripts

Python toolkit for shale energy investment analysis and operations at **Shalehaven Partners** — supporting tax-advantaged, non-operated oil & gas projects in proven basins. Developed by Michael Tanner. For questions or contributions, contact [Michael Tanner](mailto:development@shalehaven.com).

Process geospatial constraints, run production models, and evaluate economics to inform drilling decisions and investor returns.

## Core Scripts

- **`main_los.py`**  
  Line-of-Sight / geospatial visibility analysis + PnL output.  
  Screens pad locations, pipeline routes, and infrastructure conflicts.

- **`main_model.py`**  
  Core modeling engine.  
  Builds and runs reservoir, decline curve, and economic simulations.

- **`main_prod.py`**  
  Production forecasting and operational analytics.  
  Handles historical data, type curves, and cash flow projections.

## Package Modules (`shalehavenscripts/`)

- `los.py` — LOS calculations and spatial utilities
- `model.py` — Modeling logic and simulation helpers
- `production.py` — Production data processing and decline analysis
- `combocurve.py` — Combo/hybrid type curve generation

## Quick Start

```bash
# Example usage (parameters defined inside scripts)
python main_los.py
python main_model.py
python main_prod.py

