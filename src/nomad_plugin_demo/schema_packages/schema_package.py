from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from nomad.datamodel.datamodel import (
        EntryArchive,
    )
    from structlog.stdlib import (
        BoundLogger,
    )

from nomad.config import config
from nomad.datamodel.data import Schema, ArchiveSection
from nomad.datamodel.metainfo.annotations import ELNAnnotation, ELNComponentEnum
from nomad.metainfo import Quantity, SchemaPackage, Datetime, SubSection
from nomad.metainfo.data_type import m_str
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from nomad.datamodel.metainfo.plot import PlotlyFigure, PlotSection
from nomad.metainfo import MEnum, MSection, Quantity, SchemaPackage, SubSection

configuration = config.get_plugin_entry_point(
    "nomad_plugin_demo.schema_packages:schema_package_entry_point"
)

m_package = SchemaPackage()


class Entry(MSection):
    Datum = Quantity(type=str)
    Set_aktuell = Quantity(type=int)
    p_Luft_bar_ein = Quantity(type=float)
    Set_Kommentar = Quantity(type=str)
    Strom_I___A = Quantity(type=float)
    U1 = Quantity(type=float)


class NewSchemaPackage(PlotSection, Schema):
    entries = SubSection(section=Entry, repeats=True)
    name = Quantity(
        type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity)
    )

    message = Quantity(type=str)

    def normalize(self, archive: "EntryArchive", logger: "BoundLogger") -> None:
        super().normalize(archive, logger)

        if logger is not None:
            logger.info("NewSchema.normalize", parameter=configuration.parameter)

        data = archive.data.entries
        archive.metadata.entry_name = self.name
        set_id_key = "Set aktuell"
        datetime_key = "Datum"
        averaging_window = 30
        print("Experiment count in testbench:", len(data))
        data = []
        for item in archive.data.entries:
            data.append(
                {
                    "Datum": item.get("Datum"),
                    "Set_aktuell": item.get("Set_aktuell"),
                    "p_Luft_bar_ein": item.get("p_Luft_bar_ein"),
                    "Set_Kommentar": item.get("Set_Kommentar"),
                }
            )

        fig_line = px.line(data, x="Datum", y="p_Luft_bar_ein")
        plotly_figure = PlotlyFigure(figure=fig_line.to_plotly_json())

        self.figures.append(plotly_figure)
        filtered_data = [row for row in data if row["Set_Kommentar"] == "0,60V"]

        df_subset = pd.DataFrame(filtered_data)
        # set_change = df_subset[set_id_key].diff().fillna(0)
        # # Change data types to integer
        # set_change = set_change.astype("int")
        # # # Set all non-zero values to 1
        # set_change[set_change != 0] = 1
        # # # Build the cumulative sum along the rows to count changing operating modes and add to data frame
        # df_subset["set_count"] = set_change.cumsum()
        # fig_scatter = px.scatter(df_subset, x="Datum", y="Set_aktuell")
        # fig_scatter.show()
        # self.figures.append(PlotlyFigure(fig_scatter.to_json()))

        # df_subset_grouped = df_subset.drop(
        #     df_subset.columns.difference(["U1", "Strom I_A", "set_count"]), axis=1
        # )
        # grouped_subset = df_subset_grouped.groupby(["set_count"])
        # df_averaged = grouped_subset.tail(averaging_window)
        # df_averaged = (
        #     grouped_subset.tail(averaging_window)
        #     .groupby(["set_count"])
        #     .mean(numeric_only=True)
        # )
        # df_averaged = grouped_subset.apply(
        #     lambda x: x.tail(min(averaging_window, len(x) - 2)).mean(),
        #     include_groups=False,
        # )
        # df_averaged.reset_index(inplace=True)
        # fig_scatter = make_subplots(specs=[[{"secondary_y": True}]])
        # # Add traces
        # fig_scatter.add_trace(
        #     go.Scatter(x=df_averaged["set_count"], y=df_averaged["U1"], name="Voltage"),
        #     secondary_y=False,
        # )

        # fig_scatter.add_trace(
        #     go.Scatter(
        #         x=df_averaged["set_count"], y=df_averaged["Strom I_A"], name="Current"
        #     ),
        #     secondary_y=True,
        # )

        # # Add figure title
        # fig_scatter.update_layout(title_text="Double Y Axis Example")

        # # Set x-axis title
        # fig_scatter.update_xaxes(title_text="Set Count")

        # # Set y-axes titles
        # fig_scatter.update_yaxes(title_text="Voltage / V", secondary_y=False)
        # fig_scatter.update_yaxes(title_text="Current / A", secondary_y=True)
        # self.figures.append(PlotlyFigure(fig_scatter.to_json()))

        # column_dict = {col: col + "_avg" for col in df_averaged.columns}
        # df_averaged.rename(columns=column_dict, inplace=True)

        # # Get the last rows of each from the full subset to preserve corresponding index and datetimes
        # df_last_rows_of_groups = df_subset_grouped.groupby(["set_count"]).tail(1)
        # df_last_rows_of_groups.reset_index(inplace=True)
        # df_last_rows_of_groups.set_index(["set_count"], inplace=True)
        # df_combined_averaged = pd.concat([df_last_rows_of_groups, df_averaged], axis=1)
        # df_combined_averaged.set_index("index", inplace=True)
        # df_subset_extended = pd.merge(df_subset, df_combined_averaged, how="outer")

        # # Plot raw data subset and corresponding averaged
        # data = df_subset_extended
        # x_values = data[datetime_key]
        # columns = ["set_count", "U1", "U1_avg"]
        # y_values = [data[i] for i in columns]

        # modes = ["lines", "markers", "markers"]
        # markers = [
        #     {"size": 1},
        #     {"size": 5},
        #     {"size": 10},
        # ]

        # fig_go_scatter = go.Figure()
        # for i in range(len(y_values)):
        #     fig_go_scatter.add_trace(
        #         go.Scatter(
        #             x=x_values,
        #             y=y_values[i],
        #             mode=modes[i],
        #             marker=markers[i],
        #             name=columns[i],
        #         )
        #     )
        #     # fig_2.add_trace(go.Scatter(x=x_values, y=data['U1_averaged'],
        #     #                            mode='markers'))
        # fig_go_scatter.update_layout(
        #     font_family="Arial",
        #     font_color="black",
        #     font_size=14,
        # )
        # fig_go_scatter.update_xaxes(title="DateTime")
        # fig_go_scatter.update_yaxes(title="Value")
        # self.figures.append(PlotlyFigure(fig_go_scatter.to_json()))


m_package.__init_metainfo__()
