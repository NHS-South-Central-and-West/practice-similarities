import yaml

from practice_similarities.pipeline.import_data import import_data

from practice_similarities.pipeline.transform_data import (
    scale_rural_urban_classes,
    sum_staff_totals,
    calculate_patients_per_staff,
    calculate_patient_proportions,
    approximate_patient_summary_stats
)

def run_pipeline():
    """This is the main function that runs the pipeline"""
    data = import_data("data/", "practices.arrow")

    data = (
        data.lazy()
        .pipe(scale_rural_urban_classes)
        .pipe(sum_staff_totals)
        .pipe(calculate_patients_per_staff)
        .pipe(calculate_patient_proportions)
        .pipe(approximate_patient_summary_stats)
        .collect()
        )

if __name__ == "__main__":
    run_pipeline()
