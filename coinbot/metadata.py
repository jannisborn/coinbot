colors = {
    "1": "unavailable",  # Black
    1: "unavailable",
    "00000000": "missing",  # White
    "0": "missing",
    0: "missing",
    "FF00B050": "collected",  # Green
}
coin_values = [
    "1 Cent",
    "2 Cent",
    "5 Cent",
    "10 Cent",
    "20 Cent",
    "50 Cent",
    "1 Euro",
    "2 Euro",
]

eurozone = [
    "Belgien",  # Belgium
    "Deutschland",  # Germany
    "Estland",  # Estonia
    "Finnland",  # Finland
    "Frankreich",  # France
    "Griechenland",  # Greece
    "Irland",  # Ireland
    "Italien",  # Italy
    "Kroatien",  # Croatia
    "Lettland",  # Latvia
    "Litauen",  # Lithuania
    "Luxemburg",  # Luxembourg
    "Malta",  # Malta
    "Niederlande",  # Netherlands
    "Österreich",  # Austria
    "Portugal",  # Portugal
    "Slowakei",  # Slovakia
    "Slowenien",  # Slovenia
    "Spanien",  # Spain
    "Zypern",  # Cyprus
]

# To include countries that use the Euro but are not part of the Eurozone, you might add:
euro_users = [
    "Monaco",
    "Vatikan",  # Vatican City
]
translate_countries = {
    "Belgium": "Belgien",
    "Germany": "Deutschland",
    "Estonia": "Estland",
    "Finland": "Finnland",
    "France": "Frankreich",
    "Greece": "Griechenland",
    "Ireland": "Irland",
    "Italy": "Italien",
    "Croatia": "Kroatien",
    "Latvia": "Lettland",
    "Lithuania": "Litauen",
    "Luxembourg": "Luxemburg",
    "Malta": "Malta",
    "Netherlands": "Niederlande",
    "Austria": "Österreich",
    "Portugal": "Portugal",
    "Slovakia": "Slowakei",
    "Slovenia": "Slowenien",
    "Spain": "Spanien",
    "Cyprus": "Zypern",
    "Monaco": "Monaco",
    "Vatican": "Vatikan",
}

countries = eurozone + euro_users
