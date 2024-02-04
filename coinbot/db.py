import openpyxl
import pandas as pd

from coinbot.metadata import coin_values, colors, countries
from coinbot.utils import convert_to_thousands


class DataBase:
    def __init__(self, file_path: str):
        """Constructor that reads the Excel file and sets up sheets."""
        self.wb = openpyxl.load_workbook(file_path, data_only=True)
        self.deutschland_sheet = self.wb["Deutschland"]
        self.eu_sheet = self.wb["EU"]
        self.sonder_sheet = self.wb["Sondermünzen"]

        self.eu_df = self.setup_eu_dataframe()

        # TODO: Setup the Deutschland and Sondermünzen dataframes

    def cell_status(self, cell):
        """Determine the collection status based on the cell color."""
        # Assuming default colors for collected, uncollected, and unavailable
        fill_color = cell.fill.start_color.index
        if fill_color not in colors.keys():
            print(cell, fill_color)
        return colors.get(fill_color, "unknown")

    def setup_eu_dataframe(self):
        """Setup a dataframe for the EU sheet."""
        rows = list(self.eu_sheet.iter_rows(min_row=1))
        data = []
        num_countries = 0
        for i, row in enumerate(rows):
            if row[1].value in countries:
                # This row contains country names
                country = row[1].value
                # The next row contains the years
                year_row = rows[i + 1]
                years = []
                for cell in year_row[1:]:
                    if isinstance(cell.value, int) and 1999 <= cell.value <= 2023:
                        years.append(cell.value)
                    elif cell.value is None:
                        break  # Stop if the year is None
                # Loop over the years and the next 8 rows for coin values
                for year_idx, year in enumerate(years):
                    for value_idx, coin_value in enumerate(coin_values):
                        coin_row = rows[
                            (num_countries * 11) + value_idx + 2
                        ]  # Offset by 2 to skip country and year rows
                        cell = coin_row[
                            year_idx + 1
                        ]  # Offset by 1 to skip the coin value column
                        amount = (
                            cell.value if cell.value not in [None, "---", "???"] else 0
                        )
                        # print(country, year, coin_value, cell.fill.start_color.index)
                        status = self.cell_status(cell)
                        data.append([country, year, coin_value, amount, status])
                num_countries += 1

        # Create DataFrame from data
        df = pd.DataFrame(
            data, columns=["Country", "Year", "Coin Value", "Amount", "Status"]
        )
        df["Amount"] = df["Amount"].apply(convert_to_thousands).astype(int)
        df["Coin Value"] = df["Coin Value"].str.lower()
        return df
