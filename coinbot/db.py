import openpyxl
import pandas as pd
from loguru import logger

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
        self.ger_df = self.setup_ger_dataframe()
        self.sonder_df = self.setup_sonder_dataframe()
        self.df = pd.concat([self.eu_df, self.ger_df, self.sonder_df])
        self.df.update(
            self.df.drop(columns=["Name", "Link"]).map(
                lambda x: x.lower() if isinstance(x, str) else x
            )
        )

    def get_status(self):

        report_lines = []
        report_lines.append("**🤑🪙 Collection Status 🤑🪙**\n")

        # Total coins info
        total_coins = len(self.df)
        collected = len(self.df[self.df["Status"] == "collected"])
        special = len(self.df[self.df["Special"]])
        speccol = len(
            self.df[(self.df["Status"] == "collected") & (self.df["Special"])]
        )
        tr = collected / total_coins
        sr = speccol / special

        # Formatting the total and special coins information
        report_lines.append(
            f"**{self._emoji(tr)}Total coins: {total_coins}, Collected: {collected} ({tr:.2%}%)**"
        )
        report_lines.append(
            f"**{self._emoji(sr)}Special coins: {special}, Collected: {speccol} ({sr:.2%}%)**\n"
        )

        # Generating report by Country
        for country in self.df["Country"].unique():
            country_df = self.df[self.df["Country"] == country]
            total_in_country = len(country_df)
            collected_in_country = len(country_df[country_df["Status"] == "collected"])
            fraction_collected_country = (
                collected_in_country / total_in_country if total_in_country > 0 else 0
            )
            report_lines.append(
                f"{self._emoji(fraction_collected_country)} {country}: {fraction_collected_country:.2%} collected"
            )

        # Generating report by Year
        report_lines.append("")  # Add a newline for separation
        for year in sorted(self.df["Year"].unique()):
            year_df = self.df[self.df["Year"] == year]
            total_in_year = len(year_df)
            collected_in_year = len(year_df[year_df["Status"] == "collected"])
            fraction_collected_year = (
                collected_in_year / total_in_year if total_in_year > 0 else 0
            )
            report_lines.append(
                f"{self._emoji(fraction_collected_year)} {year}: {fraction_collected_year:.2%} collected"
            )

        # Joining report lines into a single string
        report = "\n".join(report_lines)
        logger.info(report)
        return report

    def cell_status(self, cell):
        """Determine the collection status based on the cell color."""
        # Assuming default colors for collected, uncollected, and unavailable
        fill_color = cell.fill.start_color.index
        if fill_color not in colors.keys():
            logger.warning(f"Unknown cell color {fill_color} in {cell}")
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
                        status = self.cell_status(cell)
                        data.append([country, year, coin_value, amount, status])
                num_countries += 1

        # Create DataFrame from data
        df = pd.DataFrame(
            data, columns=["Country", "Year", "Coin Value", "Amount", "Status"]
        )
        df["Amount"] = df["Amount"].apply(convert_to_thousands).astype(int)
        df["Coin Value"] = df["Coin Value"].str.lower()
        df["Special"] = False
        return df

    def setup_ger_dataframe(self):
        """Setup a dataframe for the Deutschland (Germany) sheet."""
        data = []
        rows = list(self.deutschland_sheet.iter_rows(min_row=1))

        # Define the start row for each 5-year block
        year_blocks = {2002: 0, 2007: 12, 2012: 24, 2017: 36, 2022: 48}

        for start_year, year_row in year_blocks.items():
            source_row = year_row + 1
            coin_value_rows = range(source_row + 1, source_row + 9)

            for year_idx in range(5):  # For each year in the block
                year = start_year + year_idx
                # Extract prägestätte marks and associate with years
                source_cell = rows[source_row][year_idx * 5 + 1 : year_idx * 5 + 6]
                sources = [cell.value for cell in source_cell]

                for value_idx, coin_row in enumerate(coin_value_rows):
                    coin_value = coin_values[value_idx]
                    for source_idx, source in enumerate(sources):
                        cell = rows[coin_row][year_idx * 5 + 1 + source_idx]
                        amount = (
                            cell.value if cell.value not in [None, "---", "???"] else 0
                        )

                        status = self.cell_status(cell)
                        data.append(
                            ["Germany", year, source, coin_value, amount, status]
                        )

        # Create DataFrame from data
        df = pd.DataFrame(data)
        df.columns = ["Country", "Year", "Source", "Coin Value", "Amount", "Status"]
        df["Amount"] = df["Amount"].apply(convert_to_thousands).astype(int)
        df["Coin Value"] = df["Coin Value"].str.lower()
        df["Special"] = False
        return df

    def setup_sonder_dataframe(self):
        """Setup a dataframe for the Sondermünzen sheet."""
        rows = list(self.sonder_sheet.iter_rows(min_row=1))
        data = []

        # Extract data for unstructured sondermünzen. Everything in the sheet has been collected.
        for i, row in enumerate(rows):
            if i < 2:
                continue

            name = row[0].value
            country = row[1].value
            year = row[2].value
            amount = int(row[3].value * 1000)  # Convert to thousands
            source = row[5].value
            cs = row[5].value is not None
            link = row[7].value
            desc = row[8].value
            state = self.cell_status(row[0])

            data.append(
                [name, country, year, "2 euro", source, amount, state, cs, desc, link]
            )

        # Create DataFrame from data
        df = pd.DataFrame(
            data,
            columns=[
                "Name",
                "Country",
                "Year",
                "Coin Value",
                "Source",
                "Amount",
                "Status",
                "Country-specific",
                "Description",
                "Link",
            ],
        )
        df["Coin Value"] = df["Coin Value"].str.lower()
        df["Special"] = True
        return df

    def _emoji(self, fraction: float) -> str:
        """Returns an emoji based on the fraction collected, one per decile."""
        if fraction == 1:
            return "✅"
        elif fraction >= 0.9:
            return "🟢"
        elif fraction >= 0.8:
            return "🟢"
        elif fraction >= 0.7:
            return "🍏"
        elif fraction >= 0.6:
            return "⚪"
        elif fraction >= 0.5:
            return "🟡"
        elif fraction >= 0.4:
            return "🟠"
        elif fraction >= 0.3:
            return "🔴"
        elif fraction >= 0.2:
            return "🟣"
        elif fraction >= 0.1:
            return "🟤"
        elif fraction >= 0:
            return "⚫"
        else:
            return "❔"
