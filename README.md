# Unified Health Analytics Framework  
### Master’s Thesis – Digital Repository

This repository contains the complete digital version of my Master’s thesis in Medical Informatics as well as the full codebase of the **Unified Health Analytics Framework** developed as part of the project.

---

##  Thesis

**Title:** Integrating Smart Ring Biometrics with Digital Questionnaires for Enhanced Health Insights
The thesis investigates how heterogeneous consumer wearable data (e.g., heart rate, HRV, SpO₂, sleep metrics) can be harmonized, processed, and aligned with standardized health questionnaires such as:

- PSQI (sleep quality)
- PSS-10 (perceived stress)
- MFI-20 (fatigue)
- 
The work focuses on:

- Cross-device data harmonization  
- Reproducible metric computation  
- Window-based biometric–questionnaire alignment  
- Coverage and stability diagnostics  
- Transparent stress and recovery index modeling  
- Free-living validation under real-world conditions  

The thesis PDF is available in this repository.

---

##  Unified Health Analytics Framework

The accompanying framework implements the complete analytical pipeline, including:

- Parsing of Apple HealthKit exports and other device data
- Standardized CSV-based data model
- Biometric metric extraction (sleep, HRV, stress proxies, activity)
- Alignment of biosignals with questionnaire windows
- Data quality diagnostics (coverage & stability)
- Visualization modules
- Modular and reproducible analysis workflows

The framework was designed with a focus on:

- Transparency  
- Reproducibility  
- Device-agnostic processing  
- Clear separation of ETL, metrics, and interpretation layers  

---

##  Research Context

The project evaluates whether consumer smart rings can provide meaningful, interpretable physiological signals when analyzed in conjunction with validated psychological and health questionnaires under non-laboratory conditions.

The goal is not to replace clinical diagnostics, but to establish a transparent analytical foundation for future digital health research.

---

## License

This repository is provided for academic and research purposes.
