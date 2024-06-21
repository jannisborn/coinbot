import json
import os

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
    "Belgium",
    "Germany",
    "Estonia",
    "Finland",
    "France",
    "Greece",
    "Ireland",
    "Italy",
    "Croatia",
    "Latvia",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Netherlands",
    "Austria",
    "Portugal",
    "San Marino",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Cyprus",
    "Vatican",
]

# To include countries that use the Euro but are not part of the Eurozone, you might add:
euro_users = ["Monaco", "Andorra"]
country_eng2ger = {
    "Andorra": "Andorra",
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
    "San Marino": "San Marino",
    "Slovakia": "Slowakei",
    "Slovenia": "Slowenien",
    "Spain": "Spanien",
    "Cyprus": "Zypern",
    "Monaco": "Monaco",
    "Vatican": "Vatikan",
}
country_ger2eng = dict(zip(country_eng2ger.values(), country_eng2ger.keys()))
country_ger2eng["Vatikanstadt"] = "Vatican"

countries = eurozone + euro_users


translation_file = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "translations.json"
)
with open(translation_file, "r") as f:
    country_to_translations = json.load(f)

countries_all_languages = []
country_to_english = {}
for c, translations in country_to_translations.items():
    countries_all_languages.append(c)
    country_to_english[c] = c
    for trans in translations:
        country_to_english[trans] = c
        countries_all_languages.append(trans)

countries_all_languages = list(set(countries_all_languages))


germany = [
    "Germany",
    "Deutschland",
    "Allemagne",
    "Alemania",
    "Germania",
    "Германия",
    "Tyskland",
    "Saksa",
    "Németország",
    "Niemcy",
    "Duitsland",
    "Tyskland",
    "Germanio",
    "Vācija",
    "Vokietija",
    "Saksamaa",
    "Njemačka",
    "Німеччина",
    "Německo",
    "Германија",
    "Németország",
    "Tedesco",
    "Alemanha",
    "Germanija",
    "Германия",
]
