"""Utilitary function for intervals, useful for temporal aggregation
methods.
"""

from datetime import timedelta

from openeo_gfmap import TemporalContext


def quintad_intervals(temporal_extent: TemporalContext) -> list:
    """Returns a list of tuples (start_date, end_date) of quintad intervals
    from the input temporal extent. Quintad intervals are intervals of
    generally 5 days, that never overlap two months.

    All months are divided in 6 quintads, where the 6th quintad might
    contain 6 days for months of 31 days.
    For the month of February, the 6th quintad is only of three days, or
    four days for the leap year.
    """
    start_date, end_date = temporal_extent.to_datetime()
    quintads = []

    current_date = start_date

    # Compute the offset of the first day on the start of the last quintad
    if start_date.day != 1:
        offset = (start_date - timedelta(days=1)).day % 5
        current_date = current_date - timedelta(days=offset)
    else:
        offset = 0

    while current_date <= end_date:
        # Get the last day of the current month
        last_day = current_date.replace(day=28) + timedelta(days=4)
        last_day = last_day - timedelta(days=last_day.day)

        # Get the last day of the current quintad
        last_quintad = current_date + timedelta(days=4)

        # Add a day if the day is the 30th and there is the 31th in the current month
        if last_quintad.day == 30 and last_day.day == 31:
            last_quintad = last_quintad + timedelta(days=1)

        # If the last quintad is after the last day of the month, then
        # set it to the last day of the month
        if last_quintad > last_day:
            last_quintad = last_day
        # In the case the last quintad is after the end date, then set it to the end date
        elif last_quintad > end_date:
            last_quintad = end_date

        quintads.append((current_date, last_quintad))

        # Set the current date to the next quintad
        current_date = last_quintad + timedelta(days=1)

    # Fixing the offset issue for intervals starting in the middle of a quintad
    quintads[0] = (quintads[0][0] + timedelta(days=offset), quintads[0][1])

    # Returns to string with the YYYY-mm-dd format
    return [
        (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        for start_date, end_date in quintads
    ]
