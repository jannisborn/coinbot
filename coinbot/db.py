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
        self.df.to_csv("tmp.csv")

    def get_status(self):

        report_lines = []
        report_lines.append("**🤑🪙 Collection Status 🤑🪙**\n")
        report_lines.append(
            "Color code:\n100% -> ✅\n>75% -> 🟢\n>60% -> 🟡\n>45% -> 🟠\n>30% -> 🔴\n>15% -> 🟤\n>0% -> ⚫\n0% -> ✖️"
        )

        # Total coins info
        df = self.df[self.df["Status"] != "unavailable"]
        total_coins = len(df)
        collected = len(df[df["Status"] == "collected"])
        special = len(df[df["Special"]])
        speccol = len(df[(df["Status"] == "collected") & (df["Special"])])
        tr = collected / total_coins
        sr = speccol / special

        # Formatting the total and special coins information
        report_lines.append(
            f"**{self._emoji(tr)}Total coins: {total_coins}, Collected: {collected} ({tr:.2%})**"
        )
        report_lines.append(
            f"**{self._emoji(sr)}Special coins: {special}, Collected: {speccol} ({sr:.2%})**\n"
        )

        # Generating report by Year
        report_lines.append("Year:")  # Add a newline for separation
        for year in sorted(df["Year"].unique()):
            year_df = df[df["Year"] == year]
            tot = len(year_df)
            col = len(year_df[year_df["Status"] == "collected"])
            fra = col / tot if tot > 0 else 0
            report_lines.append(f"{self._emoji(fra)} {year}: {fra:.2%} ({col}/{tot})")

        # Generating report by Country
        report_lines.append("\nCountries:")
        for country in df["Country"].unique():
            country_df = df[df["Country"] == country]
            tot = len(country_df)
            col = len(country_df[country_df["Status"] == "collected"])
            fra = col / tot if tot > 0 else 0
            report_lines.append(
                f"{self._emoji(fra)} {country.capitalize()}: {fra:.2%} ({col}/{tot})"
            )

        # Generating report by Coin value
        report_lines.append("\nCoin Value:")  # Add a newline for separation
        for value in [f"{x} cent" for x in [1, 2, 5, 10, 20, 50]] + [
            f"{x} euro" for x in [1, 2]
        ]:
            value_df = df[df["Coin Value"] == value]
            tot = len(value_df)
            col = len(value_df[value_df["Status"] == "collected"])
            fra = col / tot if tot > 0 else 0
            report_lines.append(
                f"{self._emoji(fra)} {value}: {fra:.2%} ({col}/{tot}) collected"
            )

        # Joining report lines into a single string
        report = "\n".join(report_lines)
        return report

    def status_delta(self, year: int, value: str, country: str):
        """
        Sends a collection status update message to the user based on the
        information of the just-collected coin.
        """

        report_lines = ["📈Updated Stats📈\n"]
        df = self.df[self.df["Status"] != "unavailable"]

        def add_change(df: pd.DataFrame, msg: str):
            total_coins = len(df)
            collected = len(df[df["Status"] == "collected"])
            tro, trn = (collected / total_coins), ((collected + 1) / total_coins)
            emo, emn = self._emoji(tro), self._emoji(trn)
            report_lines.append(f"{msg}: From {emo}{tro:.3%} ➡️ {emn}{trn:.3%}")

        # 1. Overall change
        add_change(df, msg="Total")
        # 2. Country change
        add_change(df[df.Country == country], msg=f"{country.upper()}")
        # 3. Year change
        add_change(df[df.Year == year], msg=f"{year}")
        # 4. Coin value change
        add_change(df[df["Coin Value"] == value], msg=f"{value}")
        report = "\n".join(report_lines)
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
                    if isinstance(cell.value, int) and 1999 <= cell.value <= 2030:
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

        if fraction < 0 or fraction > 1:
            return "❔"
        elif fraction == 1:
            return "✅"
        elif fraction >= 0.75:
            return "🟢"
        elif fraction >= 0.6:
            return "🟡"
        elif fraction >= 0.45:
            return "🟠"
        elif fraction >= 0.3:
            return "🔴"
        elif fraction >= 0.15:
            return "🟤"
        elif fraction > 0.0:
            return "⚫"
        elif fraction == 0:
            return "✖️"
        else:
            return "❔"
