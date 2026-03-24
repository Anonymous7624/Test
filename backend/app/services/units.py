"""Distance unit helpers — storage stays metric (km); UI uses miles."""

KM_PER_MILE = 1.609344


def miles_to_km(miles: float) -> float:
    return miles * KM_PER_MILE


def km_to_miles(km: float) -> float:
    return km / KM_PER_MILE
