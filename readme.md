# Shalehaven Scripts

Python toolkit for shale energy investment analysis and operations at **Shalehaven Partners** — supporting tax-advantaged, non-operated oil & gas projects in proven basins. Developed by Michael Tanner. For questions or contributions, contact [Michael Tanner](mailto:development@shalehaven.com).

Process geospatial constraints, run production models, and evaluate economics to inform drilling decisions and investor returns.

## Core Scripts

- **`main_los.py`**  
  Profit and loss analysis.

- **`main_prod.py`**  
  Production forecasting and operational analytics.  
 
## Package Modules (`shalehavenscripts/`)

- `los.py` — LOS calculations
- `novi.py` — Novi Labs API client for authentication and data retrieval
- `production.py` — Production data processing
- `combocurve.py` — Combo/hybrid type curve generation

## Quick Start

```bash
# Example usage (parameters defined inside scripts)
python main_los.py
python main_model.py
python main_prod.py

