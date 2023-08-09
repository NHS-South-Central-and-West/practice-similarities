"""
The :mod:`practice_similarities.pipeline` module includes functions to
import, transform and export data.
"""

from .import_data import import_data
from .transform_data import (
    scale_rural_urban_classes,
    sum_staff_totals,
    calculate_patients_per_staff,
    calculate_patient_proportions,
    approximate_patient_summary_stats
)
