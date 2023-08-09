"""Module to transform GP Practice data."""

import polars as pl

def scale_rural_urban_classes(data):
    """Function that converts RUC column to a numerical scale.

    Parameters
    ----------
    data : polars.DataFrame
        A polars DataFrame containing GP Practice data

    Raises
    ------
    TypeError
        If 'data' variable is not a polars DataFrame
    """
    if isinstance(data, pl.DataFrame):
        data = data.with_columns(
            pl.when(pl.col('ruc2') == "Urban").then(pl.lit(1))
            .when(pl.col('ruc2') == "Rural").then(pl.lit(2))
            .alias('ruc')
            )
        return data

    else:
        raise TypeError("The 'data' variable entered was not a polars DataFrame")

def sum_staff_totals(data):
    """Function that sums the total staff for each practice.

    Parameters
    ----------
    data : polars.DataFrame
        A polars DataFrame containing GP Practice data

    Raises
    ------
    TypeError
        If 'data' variable is not a polars DataFrame
    """
    if isinstance(data, pl.DataFrame):
        data = data.with_columns(
            pl.sum_horizontal(pl.col('^.*_(fte|hc)$')).alias('total_staff'),
            pl.sum_horizontal(pl.col('^(total_gp)_.*$')).alias('total_gps'),
            pl.sum_horizontal(pl.col('^(total_nurses)_.*$')).alias('total_nurses'),
            pl.sum_horizontal(pl.col('^(total_admin)_.*$')).alias('total_admins')
            )

        return data

    else:
        raise TypeError("The 'data' variable entered was not a polars DataFrame")

def calculate_patients_per_staff(data):
    """Function that calculates the number of patients per staff member.

    Parameters
    ----------
    data : polars.DataFrame
        A polars DataFrame containing GP Practice data

    Raises
    ------
    TypeError
        If 'data' variable is not a polars DataFrame
    """
    if isinstance(data, pl.DataFrame):
        data = data.with_columns(
            (pl.col('total_patients') / pl.sum_horizontal(pl.col('^.*_(fte|hc)$')))
            .alias('pts_per_staff'),
            (pl.col('total_patients') / pl.sum_horizontal(pl.col('^(total_gp)_.*$')))
            .alias('pts_per_gp'),
            (pl.col('total_patients') / pl.sum_horizontal(pl.col('^(total_nurses)_.*$')))
            .alias('pts_per_nurse'),
            (pl.col('total_patients') / pl.sum_horizontal(pl.col('^(total_admin)_.*$')))
            .alias('pts_per_admin'),
            )

        return data

    else:
        raise TypeError("The 'data' variable entered was not a polars DataFrame")


def calculate_patient_proportions(data):
    """Function that calculates the proportion of patients in each age group.

    Parameters
    ----------
    data : polars.DataFrame
        A polars DataFrame containing GP Practice data

    Raises
    ------
    TypeError
        If 'data' variable is not a polars DataFrame
    """
    if isinstance(data, pl.DataFrame):
        data = data.with_columns(
            (pl.col('total_male') / pl.col('total_patients')).alias('prop_male'),
            (pl.col('total_female') / pl.col('total_patients')).alias('prop_female'),
            (pl.sum_horizontal(pl.col('^.*_(0to4|5to14)$'))  / pl.col('total_patients'))
            .alias('prop_0to14'),
            (pl.sum_horizontal(pl.col('^.*_(15to44|45to64)$'))  / pl.col('total_patients'))
            .alias('prop_15to64'),
            (pl.sum_horizontal(pl.col('^.*_(65to74|75to84|85plus)$')) / pl.col('total_patients'))
            .alias('prop_65plus')
            )

        return data

    else:
        raise TypeError("The 'data' variable entered was not a polars DataFrame")

def approximate_patient_summary_stats(data):
    """Function that calculates the approximate mean age of patients.

    Parameters
    ----------
    data : polars.DataFrame
        A polars DataFrame containing GP Practice data

    Raises
    ------
    TypeError
        If 'data' variable is not a polars DataFrame
    """
    if isinstance(data, pl.DataFrame):
        data = data.with_columns(
            ((pl.sum_horizontal((pl.col('^.*_(0to4)$')) * 2) +
            (pl.sum_horizontal(pl.col('^.*_(5to14)$')) * 9) +
            (pl.sum_horizontal(pl.col('^.*_(15to44)$')) * 30) +
            (pl.sum_horizontal(pl.col('^.*_(45to64)$')) * 55) +
            (pl.sum_horizontal(pl.col('^.*_(65to74)$')) * 70) +
            (pl.sum_horizontal(pl.col('^.*_(75to84)$')) * 80) +
            (pl.sum_horizontal(pl.col('^.*_(85plus)$')) * 90)) /
            pl.col('total_patients'))
            .alias('approx_mean_age')
            )

        return data

    else:
        raise TypeError("The 'data' variable entered was not a polars DataFrame")
