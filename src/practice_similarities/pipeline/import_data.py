"""Module to import GP Practice data."""

import polars as pl

def import_data(data_dir, data_file):
    """Function to import GP Practice data.

    Parameters
    ----------
    data_dir : str
        The directory that contains the data files
    data_file : str
        The name of the file to import

    Returns
    -------
    polars.DataFrame
        A polars DataFrame containing the data

    Raises
    ------
    TypeError
        If 'data_dir' variable is not a str
    TypeError
        If 'data_file' variable is not a str
    TypeError
        If there is an error with the 'data_dir' and 'data_file' variables
    """
    if isinstance(data_dir, str) and isinstance(data_file, str):
        raw_data = (
            pl.scan_ipc(data_dir + data_file)
            # fill nulls for staff columns with 0
            .with_columns(pl.col('^.*_(fte|hc)$').fill_null(0))
            # drop nulls for other columns
            .drop_nulls()
            .filter(
                (pl.col('total_patients') != 0) &
                (pl.all_horizontal(pl.col('^.*_(fte|hc)$') != 0))
                )
            .collect()
            )

        return raw_data

    elif not isinstance(data_dir, str):
        raise TypeError("The 'data_dir' variable entered was not a string")

    elif not isinstance(data_file, str):
        raise TypeError("The 'data_file' variable entered was not a string")

    else:
        raise TypeError("There was an error with the 'data_dir' and 'data_file' variables")
