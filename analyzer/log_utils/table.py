import csv
from typing import Optional
from loguru import logger
from prettytable import PrettyTable

from analyzer.log_utils.data_utils import Percentile, Statistics


class Table:
    def __init__(self, header: list):
        self.header = header
        self.rows = []

    def add_row(self, row: list):
        assert len(row) == len(self.header), "row and header length mismatch"
        self.rows.append(row)

    def pretty_print(self):
        table = PrettyTable()
        table.field_names = self.header

        for row in self.rows:
            table.add_row(row)

        print(table)

    def output_csv(self, output_file: str):
        with open(output_file, "w", newline="") as fp:
            writer = csv.writer(fp)
            writer.writerow(self.header)
            for row in self.rows:
                writer.writerow(row)

    @staticmethod
    def new_matrix(name: str):
        header = [name]

        for p in Percentile:
            if p is not Percentile.Min:
                header.append(p.name)

        return Table(header)

    def add_data(self, name: str, data_format: Optional[str], data: list):
        self.add_stat(name, data_format, Statistics(data))

    def add_stat(self, name: str, data_format: Optional[str], stat: Statistics):
        try:
            row = [name]

            for p in Percentile:
                # skip Min column (header omits it)
                if p is Percentile.Min:
                    continue

                if p in [Percentile.Avg, Percentile.Cnt]:
                    v = stat.get(p)
                else:
                    v = stat.get(p, data_format)

                # represent missing values clearly
                if v is None:
                    row.append("N/A")
                else:
                    row.append(v)

            self.add_row(row)
        except Exception as e:
            try:
                sdict = stat.__dict__
            except Exception:
                sdict = {}
            logger.warning(f"Cannot add stat for '{name}': {e}, {sdict}")
